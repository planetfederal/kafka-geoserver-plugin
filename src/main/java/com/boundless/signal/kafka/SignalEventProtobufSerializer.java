package com.boundless.signal.kafka;

import com.boundless.signal.geoserver.wfs.SignalEvent;
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
public class SignalEventProtobufSerializer implements Serializer<SignalEvent> {

  public static final String KAFKA_FORMAT = "pbf";

  private static final Logger LOG = Logging.getLogger(SignalEventProtobufSerializer.class);

  private static final WKBWriter GEOMETRY_WRITER = new WKBWriter();

  @Override
  public byte[] serialize(String string, SignalEvent event) {
    SimpleFeature feature = event.getFeature();
    Geometry geom = (Geometry) feature.getDefaultGeometry();
    SpatialConnect.Feature.Builder featureBuilder = SpatialConnect.Feature.newBuilder()
            .setFid(feature.getID())
            .setGeometryType(geom.getGeometryType())
            .setGeometry(ByteString.copyFrom(GEOMETRY_WRITER.write(geom)))
            .putAllProperties(feature.getProperties().stream()
                    // Ignore null properties
                    .filter(p -> p.getName() != null && p.getValue() != null)
                    // Convert the list of Properties to a simple map
                    .collect(Collectors.toMap(
                            // Property Name
                            p -> p.getName().getLocalPart(),
                            // Property Value
                            p -> {
                              if (p.getValue() != null) {
                                return p.getValue().toString();
                              } else {
                                return null;
                              }
                            })));
    SpatialConnect.Operation operation = SpatialConnect.Operation.newBuilder()
            .setOperation(event.getOperation())
            .setLayer(event.getLayerName())
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
