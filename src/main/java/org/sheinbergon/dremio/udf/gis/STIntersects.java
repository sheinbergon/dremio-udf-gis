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

@FunctionTemplate(
    name = "ST_Intersects",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.INTERNAL)
public class STIntersects implements SimpleFunction {

  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput1;

  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput2;

  @Output
  org.apache.arrow.vector.holders.NullableBitHolder output;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
    if (org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.areHoldersSet(binaryInput1, binaryInput2)) {
      org.locationtech.jts.geom.Geometry geom1 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toGeometry(binaryInput1);
      org.locationtech.jts.geom.Geometry geom2 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toGeometry(binaryInput2);
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.verifyMatchingSRIDs(geom1, geom2);
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.setBooleanValue(output, geom1.intersects(geom2));
    } else {
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.markHolderNotSet(output);
    }
  }
}
