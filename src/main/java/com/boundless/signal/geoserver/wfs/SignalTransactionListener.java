package com.boundless.signal.geoserver.wfs;

import com.boundless.signal.kafka.SignalProducer;
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

public class SignalTransactionListener implements TransactionPlugin {

  private static Logger log = Logging.getLogger(SignalTransactionListener.class);

  static final String SIGNAL_EVENTS = "SIGNAL_EVENTS";

  private final SignalProducer signalProducer;

  public SignalTransactionListener(SignalProducer signalProducer) {
    log.log(Level.FINEST, "\n\n***** SignalTransactionListener Constructor. *****\n\n");
    this.signalProducer = signalProducer;
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
  public void afterTransaction(TransactionType request, TransactionResponseType result, boolean committed) {
    log.log(Level.FINE, "\n\n***** SignalTransactionListener.afterTransaction *****\n\n");
    if (committed && request.getExtendedProperties().containsKey(SIGNAL_EVENTS)) {
      for (SignalEvent event : (List<SignalEvent>) request.getExtendedProperties().get(SIGNAL_EVENTS)) {
        this.signalProducer.send(event);
      }
    }
  }

  @Override
  public int getPriority() {
    return 0;
  }

  @Override
  public void dataStoreChange(TransactionEvent event) throws WFSException {
    TransactionEventType type = event.getType();
    log.log(Level.FINE, "\n\n***** SignalTransactionListener.dataStoreChange {0} *****\n\n", type);
    if (TransactionEventType.POST_INSERT.equals(type)) {
      createSignalEvents("insert", event);
    } else if (TransactionEventType.POST_UPDATE.equals(type)) {
      createSignalEvents("update", event);
    } else if (TransactionEventType.PRE_DELETE.equals(type)) {
      createSignalEvents("delete", event);
    }
  }

  private void createSignalEvents(String operation, TransactionEvent event) {
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
        events.add(new SignalEvent(operation, event.getLayerName(), feature));
      }
    }
  }

}
