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

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import org.apache.arrow.memory.ArrowBuf;

import javax.inject.Inject;

@FunctionTemplate(
    name = "ST_Point",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STPoint implements SimpleFunction {

  @Param
  org.apache.arrow.vector.holders.Float8Holder longitudeInput;

  @Param
  org.apache.arrow.vector.holders.Float8Holder latitudeInput;

  @Param
  org.apache.arrow.vector.holders.NullableIntHolder sridInput;

  @Output
  org.apache.arrow.vector.holders.NullableVarBinaryHolder output;

  @Inject
  ArrowBuf buffer;

  public void setup() {
  }

  public void eval() {
    double longitude = longitudeInput.value;
    double latitude = latitudeInput.value;
    com.esri.core.geometry.SpatialReference srid = com.esri.core.geometry.SpatialReference.create(srid());
    com.esri.core.geometry.Point point = new com.esri.core.geometry.Point(longitude, latitude);
    com.esri.core.geometry.ogc.OGCPoint geometry = new com.esri.core.geometry.ogc.OGCPoint(point, srid);
    byte[] bytes = FunctionHelpersXL.toBinary(geometry);
    buffer = buffer.reallocIfNeeded(bytes.length);
    FunctionHelpersXL.populate(bytes, buffer, output);
  }

  private int srid() {
    if (FunctionHelpersXL.isHolderSet(sridInput)) {
      return sridInput.value;
    } else {
      return FunctionHelpersXL.DEFAULT_SRID;
    }
  }
}