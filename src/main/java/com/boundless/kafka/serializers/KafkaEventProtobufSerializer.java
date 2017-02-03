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
package com.boundless.kafka.serializers;

import com.boundless.kafka.KafkaEvent;
import com.boundlessgeo.spatialconnect.schema.SpatialConnect;
import com.google.protobuf.ByteString;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serializer;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Serializes Operation protobuf to Kafka topic.
 */
public class KafkaEventProtobufSerializer implements Serializer<KafkaEvent> {

  public static final String KAFKA_FORMAT = "pbf";

  private static final Logger LOG = Logging.getLogger(KafkaEventProtobufSerializer.class);

  private static final WKBWriter GEOMETRY_WRITER = new WKBWriter();

  @Override
  public byte[] serialize(String string, KafkaEvent event) {
    SimpleFeature feature = event.getFeature();
    Geometry geom = (Geometry) feature.getDefaultGeometry();
    SpatialConnect.Feature.Builder featureBuilder = SpatialConnect.Feature.newBuilder()
            .setFid(feature.getID())
            .putAllProperties(feature.getProperties().stream()
                    // Skip the geometry property, it will be written later.
                    .filter(p -> !p.getName().equals(feature.getDefaultGeometryProperty().getName()))
                    // Ignore null properties
                    .filter(p -> p.getName() != null && p.getValue() != null)
                    // Convert the list of Properties to a simple map
                    .collect(Collectors.toMap(
                            // Property Name
                            p -> p.getName().getLocalPart(),
                            // Property Value
                            p -> {
                              // Right now we are just converting all the properties to String.
                              // TODO: Improve the protobuf to support multiple types.
                              if (p.getValue() != null) {
                                return p.getValue().toString();
                              } else {
                                return null;
                              }
                            })));

    // Write the geometry as WKB if it exists
    if (geom != null) {
      featureBuilder.setGeometry(ByteString.copyFrom(GEOMETRY_WRITER.write(geom)));
    }

    SpatialConnect.Operation operation = SpatialConnect.Operation.newBuilder()
            .setOperation(event.getOperation())
            .setSource(event.getLayerName())
            .setFeature(featureBuilder)
            .build();

    if (LOG.isLoggable(Level.FINE)) {
      try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
        operation.writeTo(stream);
        LOG.log(Level.FINE, "Serializing Kafka Message as Protobuf:\n{0}", stream.toString());
      } catch (IOException ex) {
        LOG.log(Level.SEVERE, "Unable to serialize Kafka Message as Protobuf.", ex);
      }
    }

    return operation.toByteArray();
  }

  @Override
  public void configure(Map<String, ?> map, boolean bln) {
    // Do nothing.
  }

  @Override
  public void close() {
    // Do nothing.
  }

}
