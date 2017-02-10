/**
 * Copyright 2015-2017 Boundless, http://boundlessgeo.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License
 */
package com.boundless.kafka.geoserver.wfs;

import com.boundless.kafka.KafkaEvent;
import com.boundless.kafka.GSKafkaProducer;
import com.boundlessgeo.spatialconnect.schema.SpatialConnect;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.namespace.QName;
import net.opengis.wfs.TransactionResponseType;
import net.opengis.wfs.TransactionType;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.NamespaceInfo;
import org.geoserver.wfs.TransactionEvent;
import org.geoserver.wfs.TransactionEventType;
import org.geoserver.wfs.TransactionPlugin;
import org.geoserver.wfs.WFSException;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;

/**
 * WFS-T listener that will collect committed transactions and send events to Kafka for each affected feature. Using the
 * dataStoreChange handler we can build an update/insert/delete event for the affected features. We add these events to
 * a list in the extendProperties on the TransactionRequest. Then use the afterTransaction handler to make sure the
 * whole transaction was committed before sending the events to Kafka.
 */
public class KafkaTransactionListener implements TransactionPlugin {

  private static final Logger LOG = Logging.getLogger(KafkaTransactionListener.class);

  /**
   * Name of the property used to store the events sent to Kafka in the transaction request's extended properties.
   */
  static final String SIGNAL_EVENTS = "SIGNAL_EVENTS";

  private final GSKafkaProducer signalProducer;

  private final Catalog catalog;

  public KafkaTransactionListener(GSKafkaProducer signalProducer, Catalog catalog) {
    this.signalProducer = signalProducer;
    this.catalog = catalog;
    LOG.log(Level.INFO, "KafkaTransactionListener Started.");
  }

  /**
   * This method is fired after the entire WFS-T request is processed.
   *
   * @param request
   * @param result
   * @param committed Whether or not the entire transaction was successful
   */
  @Override
  public void afterTransaction(TransactionType request, TransactionResponseType result, boolean committed) {
    LOG.log(Level.INFO, "KafkaTransactionListener.afterTransaction (committed={0})", committed);
    if (committed && request.getExtendedProperties().containsKey(SIGNAL_EVENTS)) {
      List<KafkaEvent> events = (List<KafkaEvent>) request.getExtendedProperties().get(SIGNAL_EVENTS);
      LOG.log(Level.FINE, "Sending {0} events to kafka.", events.size());
      for (KafkaEvent event : events) {
        this.signalProducer.send(event);
      }
    }
  }

  /**
   * This method is fired for each INSERT/UPDATE/DELETE in the WFS-T request. We build an event to send to the Kafka
   * producer for each affected feature.
   *
   * @param event
   * @throws WFSException
   */
  @Override
  public void dataStoreChange(TransactionEvent event) throws WFSException {
    TransactionEventType type = event.getType();

    if (LOG.isLoggable(Level.INFO)) {
      LOG.log(Level.INFO, "KafkaTransactionListener.dataStoreChange {0} {1}", new Object[]{event.getLayerName(), type});
    }

    if (TransactionEventType.POST_INSERT.equals(type)) {
      createSignalEvents(SpatialConnect.OperationType.INSERT, event);
    } else if (TransactionEventType.POST_UPDATE.equals(type)) {
      createSignalEvents(SpatialConnect.OperationType.UPDATE, event);
    } else if (TransactionEventType.PRE_DELETE.equals(type)) {
      createSignalEvents(SpatialConnect.OperationType.DELETE, event);
    }
  }

  private void createSignalEvents(SpatialConnect.OperationType operation, TransactionEvent event) {
    try (SimpleFeatureIterator it = event.getAffectedFeatures().features()) {
      while (it.hasNext()) {
        SimpleFeature feature = it.next();
        Map extendedProperties = event.getRequest().getExtendedProperties();
        List<KafkaEvent> events;
        if (extendedProperties.containsKey(SIGNAL_EVENTS)) {
          events = (List<KafkaEvent>) extendedProperties.get(SIGNAL_EVENTS);
        } else {
          events = new ArrayList<>();
          extendedProperties.put(SIGNAL_EVENTS, events);
        }
        // Storing events in a list so they can be sent only after the entire transaction is successful.
        events.add(new KafkaEvent(operation, getLayerName(event.getLayerName()), feature));
      }
    }
  }

  /**
   * For some reason WFS-T Inserts do not populate the workspace prefix. It's important to have the workspace prefix for
   * further down the chain. Fetch it from the Catalog.
   *
   * @param name layer name
   * @return layer name with prefix populated
   */
  private QName getLayerName(QName name) {
    if (name.getPrefix() == null || name.getPrefix().isEmpty()) {
      LOG.info("Layer prefix not set. Fetching namespace from catalog.");
      NamespaceInfo namespace = this.catalog.getNamespaceByURI(name.getNamespaceURI());
      if (namespace != null) {
        return new QName(name.getNamespaceURI(), name.getLocalPart(), namespace.getPrefix());
      } else {
        LOG.log(Level.SEVERE, "Unable to fetch namespace from catalog for URI ({0})", name.getNamespaceURI());
      }
    }

    return name;
  }

  @Override
  public TransactionType beforeTransaction(TransactionType request) throws WFSException {
    return request;
  }

  @Override
  public void beforeCommit(TransactionType request) throws WFSException {
    // Do Nothing
  }

  @Override
  public int getPriority() {
    return 0;
  }

}
