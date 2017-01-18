package com.boundless.signal.kafka;

import com.boundless.signal.geoserver.wfs.SignalEvent;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.geotools.util.logging.Logging;

public class SignalProducer {

  private static Logger log = Logging.getLogger(SignalProducer.class);

  private static final String SERVER_PROPERTY_NAME = "bootstrap.servers";

  private Producer<String, String> producer;

  public SignalProducer(Properties properties) {
    if (System.getProperty(SERVER_PROPERTY_NAME) != null) {
      properties.setProperty(SERVER_PROPERTY_NAME, System.getProperty(SERVER_PROPERTY_NAME));
    }
    this.producer = new KafkaProducer<>(properties);
  }

  public void send(SignalEvent event) {
    log.log(Level.FINE, "\n\n***** SignalProducer.send *****\n\n");
    try {
      String value = event.toJson();
      log.log(Level.FINE, "\n\n***** {0} *****\n\n", value);
      this.producer.send(new ProducerRecord<>(event.getLayerName(), value));
    } catch (IOException ex) {
      log.log(Level.SEVERE, "Unable to convert SignalEvent to JSON.", ex);
    }
  }
}
