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
    name = "ST_Angle",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STAnglePoint implements SimpleFunction {
  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput1;
  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput2;
  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput3;
  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput4;

  @Output
  org.apache.arrow.vector.holders.Float8Holder output;

  public void setup() {
  }

  public void eval() {
    org.locationtech.jts.geom.Point p1 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toPoint(binaryInput1);
    org.locationtech.jts.geom.Point p2 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toPoint(binaryInput2);
    org.locationtech.jts.geom.Point p3 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toPoint(binaryInput3);
    org.locationtech.jts.geom.Coordinate intersection = null;

    if (org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.isHolderSet(binaryInput4)) {
      org.locationtech.jts.geom.Point p4 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toPoint(binaryInput4);
      org.locationtech.jts.geom.LineSegment s1 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toLineSegment(p1, p2);
      org.locationtech.jts.geom.LineSegment s2 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toLineSegment(p3, p4);
      intersection = s1.lineIntersection(s2);
    } else {
      intersection = p2.getCoordinate();
    }
    output.value = org.locationtech.jts.algorithm.Angle.angleBetween(p1.getCoordinate(), intersection, p3.getCoordinate());
  }
}
