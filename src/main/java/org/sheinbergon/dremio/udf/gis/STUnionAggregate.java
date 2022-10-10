/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.sheinbergon.dremio.udf.gis;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;

import javax.inject.Inject;

@FunctionTemplate(
    name = "ST_Union",
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public class STUnionAggregate implements AggrFunction {
  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder input;

  @Workspace
  org.apache.arrow.vector.holders.NullableVarBinaryHolder value;

  @Workspace
  org.apache.arrow.vector.holders.NullableBitHolder indicator;

  @Output
  org.apache.arrow.vector.holders.NullableVarBinaryHolder output;

  @Inject
  org.apache.arrow.memory.ArrowBuf valueBuffer;

  @Inject
  org.apache.arrow.memory.ArrowBuf outputBuffer;

  @Override
  public void setup() {
    value = new org.apache.arrow.vector.holders.NullableVarBinaryHolder();
    indicator = new org.apache.arrow.vector.holders.NullableBitHolder();
    org.locationtech.jts.geom.Geometry empty = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.emptyGeometry();
    byte[] bytes = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toBinary(empty);
    valueBuffer = valueBuffer.reallocIfNeeded(bytes.length);
    org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.populate(bytes, valueBuffer, value);
    org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.markHolderSet(value);
    org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.setValueFalse(indicator);
  }

  @Override
  public void add() {
    if (org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.isHolderSet(input)) {
      org.locationtech.jts.geom.Geometry geom = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toGeometry(input);
      org.locationtech.jts.geom.Geometry accumulated = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toGeometry(value);
      org.locationtech.jts.geom.Geometry union = accumulated.union(geom);
      byte[] bytes = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toBinary(union);
      valueBuffer = valueBuffer.reallocIfNeeded(bytes.length);
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.populate(bytes, valueBuffer, value);
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.setValueTrue(indicator);
    }
  }

  @Override
  public void output() {
    if (org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.isValueTrue(indicator)) {
      org.locationtech.jts.geom.Geometry union = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toGeometry(value);
      byte[] bytes = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toBinary(union);
      outputBuffer = outputBuffer.reallocIfNeeded(bytes.length);
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.populate(bytes, outputBuffer, output);
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.markHolderSet(output);
    } else {
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.markHolderNotSet(output);
    }
  }

  @Override
  public void reset() {
    org.locationtech.jts.geom.Geometry empty = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.emptyGeometry();
    byte[] bytes = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toBinary(empty);
    valueBuffer = valueBuffer.reallocIfNeeded(bytes.length);
    org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.populate(bytes, valueBuffer, value);
    org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.markHolderSet(value);
    org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.setValueFalse(indicator);
  }
}