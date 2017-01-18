# Signal GeoServer Plugin

Plugin for GeoServer adding a WFS-T transaction listener that will send a message to Kafka for each affected feature.

## Running the plugin
Build a jar and drop it in your `geoserver/WEB-INF/lib` directory.

Use `bootstrap.servers` environment variable when running GeoServer to configure the location of your Kafka. Defaults to `localhost:9092`.

The plugin will send events to layer specific topics named `<workspace-prefix>.<layer-name>`. It assumes that kafka is using the setting to auto-create new topics or that the topics will already exist.

POST a WFS-T request to your GeoServer WFS endpoint (http://localhost:8080/geoserver/wfs) and the plugin should fire message(s) to Kafka. See the examples folder for some sample requests.

To verify that the messages are being sent to the kafka topic, you can use the command line consumer:
```
bin/kafka-console-consumer.sh --new-consumer --from-beginning --bootstrap-server localhost:9092 --topic signal.test
```

## Message Format
Currently, an event is a simple json structure that contains the **operation** (insert, update, or delete), **layer** (same as the topic name), and **feature** (GeoJSON formatted).

Ex:
```json
{
  "operation": "insert",
  "layer": "boundless.countries",
  "feature": {
    "type":"Feature",
    "geometry":{"type":"MultiPolygon","coordinates":[[[[0.0,0.0],[0.0,20],[-20,20],[-20,0.0],[0.0,0.0]]]]},
    "properties":{"sovereignt":"Country","admin":"Test Country"},
    "id":"countries.178"
  }
}
```
