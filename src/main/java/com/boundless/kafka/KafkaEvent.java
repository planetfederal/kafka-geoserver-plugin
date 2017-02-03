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

import com.boundlessgeo.spatialconnect.schema.SpatialConnect;
import java.util.logging.Logger;
import javax.xml.namespace.QName;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Represents the change made to a single feature during a WFS-T operation. Pass this object to the Kafka serializer.
 */
public class KafkaEvent {

  private static final Logger LOG = Logging.getLogger(KafkaEvent.class);

  /**
   * The type of operation this event represents (INSERT, UPDATE, DELETE).
   */
  private SpatialConnect.OperationType operation;

  /**
   * The layer name being operated on.
   */
  private QName layer;

  /**
   * The affected feature.
   */
  private SimpleFeature feature;

  public KafkaEvent(SpatialConnect.OperationType operation, QName layer, SimpleFeature feature) {
    this.operation = operation;
    this.layer = layer;
    this.feature = feature;
  }

  /**
   * Assemble the layer name in the format workspacePrefix.layerName
   *
   * @return the layer name
   */
  public String getLayerName() {
    if (this.layer.getPrefix() != null && !this.layer.getPrefix().isEmpty()) {
      return this.layer.getPrefix() + "." + this.layer.getLocalPart();
    } else {
      LOG.warning("Layer prefix is not set. Kafka messages maybe sent to the wrong topic.");
      return this.layer.getLocalPart();
    }
  }

  /**
   * The type of operation this event represents (INSERT, UPDATE, DELETE).
   *
   * @return the operation
   */
  public SpatialConnect.OperationType getOperation() {
    return operation;
  }

  /**
   * The type of operation this event represents (INSERT, UPDATE, DELETE).
   *
   * @param operation the operation to set
   */
  public void setOperation(SpatialConnect.OperationType operation) {
    this.operation = operation;
  }

  /**
   * The layer name being operated on.
   *
   * @return the layer
   */
  public QName getLayer() {
    return layer;
  }

  /**
   * The layer name being operated on.
   *
   * @param layer the layer to set
   */
  public void setLayer(QName layer) {
    this.layer = layer;
  }

  /**
   * The affected feature.
   *
   * @return the feature
   */
  public SimpleFeature getFeature() {
    return feature;
  }

  /**
   * The affected feature.
   *
   * @param feature the feature to set
   */
  public void setFeature(SimpleFeature feature) {
    this.feature = feature;
  }
}
