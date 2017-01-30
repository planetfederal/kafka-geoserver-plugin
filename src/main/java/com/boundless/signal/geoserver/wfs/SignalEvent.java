package com.boundless.signal.geoserver.wfs;

import com.boundlessgeo.spatialconnect.schema.SpatialConnect;
import javax.xml.namespace.QName;
import org.opengis.feature.simple.SimpleFeature;

public class SignalEvent {

  private SpatialConnect.OperationType operation;

  private QName layer;

  private SimpleFeature feature;

  public SignalEvent(SpatialConnect.OperationType operation, QName layer, SimpleFeature feature) {
    this.operation = operation;
    this.layer = layer;
    this.feature = feature;
  }

  public String getLayerName() {
    // I was using prefix.localPart but inserts do not populate the prefix. So there would be an inconsistent behavior
    // between inserts and updates.
    return this.layer.getLocalPart();
  }

  /**
   * @return the operation
   */
  public SpatialConnect.OperationType getOperation() {
    return operation;
  }

  /**
   * @param operation the operation to set
   */
  public void setOperation(SpatialConnect.OperationType operation) {
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
