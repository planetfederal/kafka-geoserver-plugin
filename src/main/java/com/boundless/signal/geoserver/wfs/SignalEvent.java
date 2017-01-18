package com.boundless.signal.geoserver.wfs;

import java.io.IOException;
import java.io.StringWriter;
import javax.xml.namespace.QName;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;

public class SignalEvent {

  private String operation;

  private QName layer;

  private SimpleFeature feature;

  public SignalEvent(String operation, QName layer, SimpleFeature feature) {
    this.operation = operation;
    this.layer = layer;
    this.feature = feature;
  }

  public String toJson() throws IOException {
    FeatureJSON fjson = new FeatureJSON();
    StringWriter writer = new StringWriter();

    fjson.writeFeature(this.feature, writer);

    String featureJson = writer.toString();

    return "{\n  \"operation\": \"" + this.operation + "\","
            + "\n  \"layer\": \"" + this.getLayerName() + "\","
            + "\n  \"feature\": " + featureJson + "\n}";
  }

  public String getLayerName() {
    return this.layer.getPrefix() + "." + this.layer.getLocalPart();
  }

  /**
   * @return the operation
   */
  public String getOperation() {
    return operation;
  }

  /**
   * @param operation the operation to set
   */
  public void setOperation(String operation) {
    this.operation = operation;
  }

  /**
   * @return the layer
   */
  public QName getLayer() {
    return layer;
  }

  /**
   * @param layer the layer to set
   */
  public void setLayer(QName layer) {
    this.layer = layer;
  }

  /**
   * @return the feature
   */
  public SimpleFeature getFeature() {
    return feature;
  }

  /**
   * @param feature the feature to set
   */
  public void setFeature(SimpleFeature feature) {
    this.feature = feature;
  }
}
