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
 * the applicationContext.xml (currently kafka-producer.properties). The "bootstrap.servers" property can be specified
 * as a Java system property. The "kafkaFormat" Java system property can be used to change the format of the messages
 * sent to Kafka (json or pbf).
 */
public class GSKafkaProducer {

  /**
   * The Java system property to set the Kafka serialization format.
   */
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
