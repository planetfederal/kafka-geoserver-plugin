package com.boundless.signal.kafka;

import com.boundless.signal.geoserver.wfs.SignalEvent;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.common.serialization.Serializer;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.util.logging.Logging;

/**
 * Serializes Operation JSON to Kafka topic.
 */
public class SignalEventJsonSerializer implements Serializer<SignalEvent> {

  public static final String KAFKA_FORMAT = "json";

  private static final Logger LOG = Logging.getLogger(SignalEventJsonSerializer.class);

  @Override
  public byte[] serialize(String string, SignalEvent event) {
    try {
      String json = toJson(event);
      LOG.log(Level.FINE, "Serializing Kafka Message as JSON:\n{0}", json);
      return json.getBytes();
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Unable to serialize Kafka Message as JSON.", ex);
      return null;
    }
  }

  private String toJson(SignalEvent event) throws IOException {
    FeatureJSON fjson = new FeatureJSON();

    // Normally I would use a library to build this json. Since it is not a direct mapping from SimpleFeature to
    // GeoJSON, I am using the FeatureJSON to build the feature GeoJSON String. I would have to convert a SimpleFeature
    // to a GeoJSON String, then to a json builder object, then add that to the root level json builder object, and
    // finally convert it all back to String to send over the wire. Seems like to much overhead for such a simple json
    // object. I would consider changing the approach if the json gets much more complex.
    return "{\n  \"operation\": \"" + event.getOperation().name() + "\","
            + "\n  \"layer\": \"" + event.getLayerName() + "\","
            + "\n  \"feature\": " + fjson.toString(event.getFeature()) + "\n}";
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
