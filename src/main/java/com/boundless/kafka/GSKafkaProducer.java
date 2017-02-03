package com.boundless.kafka;

import com.boundless.kafka.serializers.KafkaEventJsonSerializer;
import com.boundless.kafka.serializers.KafkaEventProtobufSerializer;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.geotools.util.logging.Logging;

/**
 * A Kafka producer that will send our events to the appropriate topic. Producer configuration location is specified in
 * the applicationContext.xml (currently kafka-producer.properties). The bootstrap.servers property can be specified as
 * a system property. The kafkaFormat system property can be used to change the format of the messages sent to Kafka
 * (json or pbf).
 */
public class GSKafkaProducer {

  public static final String KAFKA_FORMAT_PROPERTY_NAME = "kafkaFormat";

  private static final Logger LOG = Logging.getLogger(KafkaProducer.class);

  private Producer<String, KafkaEvent> producer;

  public GSKafkaProducer(Properties properties) {
    // Set the bootstrap.servers from the system property if present
    if (System.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) != null) {
      properties.setProperty(
              ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
              System.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
      );
    }

    // Configure the correct serializer based on the system property if present
    String kafkaFormat = System.getProperty(KAFKA_FORMAT_PROPERTY_NAME);
    if (kafkaFormat != null) {
      if (KafkaEventJsonSerializer.KAFKA_FORMAT.equalsIgnoreCase(kafkaFormat)) {
        configureSerializer(properties, KafkaEventJsonSerializer.class);
      } else if (KafkaEventProtobufSerializer.KAFKA_FORMAT.equalsIgnoreCase(kafkaFormat)) {
        configureSerializer(properties, KafkaEventProtobufSerializer.class);
      } else {
        LOG.log(Level.WARNING, "Unrecognized kafkaFormat ({0}). Defaulting to {1}.",
                new Object[]{kafkaFormat, KafkaEventProtobufSerializer.KAFKA_FORMAT});
        configureSerializer(properties, KafkaEventProtobufSerializer.class);
      }
    }

    this.producer = new KafkaProducer<>(properties);
  }

  public void send(KafkaEvent event) {
    LOG.log(Level.FINE, "GSKafkaProducer.send:\n{0}", event.toString());
    this.producer.send(new ProducerRecord<>(event.getLayerName(), event));
  }

  /**
   * Sets the appropriate Kafka value serializer.
   *
   * @param properties ProducerConfig properties
   * @param serializer Kafka Serializer
   */
  private void configureSerializer(Properties properties, Class serializer) {
    LOG.log(Level.INFO, "Using KafkaSerializer: {0}", serializer.getCanonicalName());
    properties.setProperty(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            serializer.getCanonicalName()
    );
  }
}
