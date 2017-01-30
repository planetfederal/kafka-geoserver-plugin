package com.boundless.signal.geoserver.wfs;

import com.boundless.signal.kafka.SignalProducer;
import com.boundlessgeo.spatialconnect.schema.SpatialConnect;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.opengis.wfs.TransactionResponseType;
import net.opengis.wfs.TransactionType;
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
public class SignalTransactionListener implements TransactionPlugin {

  private static Logger LOG = Logging.getLogger(SignalTransactionListener.class);

  /**
   * Name of the property used to store the events sent to kafka in the transaction request's extended properties.
   */
  static final String SIGNAL_EVENTS = "SIGNAL_EVENTS";

  private final SignalProducer signalProducer;

  public SignalTransactionListener(SignalProducer signalProducer) {
    LOG.log(Level.FINE, "SignalTransactionListener Constructor.");
    this.signalProducer = signalProducer;
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
    LOG.log(Level.INFO, "SignalTransactionListener.afterTransaction (committed={0})", committed);
    if (committed && request.getExtendedProperties().containsKey(SIGNAL_EVENTS)) {
      for (SignalEvent event : (List<SignalEvent>) request.getExtendedProperties().get(SIGNAL_EVENTS)) {
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
      LOG.log(Level.INFO, "SignalTransactionListener.dataStoreChange {0} {1}", new Object[]{event.getLayerName(), type});
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
        List<SignalEvent> events;
        if (extendedProperties.containsKey(SIGNAL_EVENTS)) {
          events = (List<SignalEvent>) extendedProperties.get(SIGNAL_EVENTS);
        } else {
          events = new ArrayList<>();
          extendedProperties.put(SIGNAL_EVENTS, events);
        }
        // Storing events in a list so they can be sent only after the entire transaction is successful.
        events.add(new SignalEvent(operation, event.getLayerName(), feature));
      }
    }
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
