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

import javax.inject.Inject;

@FunctionTemplate(
    name = "ST_Simplify",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.INTERNAL,
    costCategory = FunctionTemplate.FunctionCostCategory.COMPLEX)
public class STSimplify implements SimpleFunction {

  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput;

  @Param
  org.apache.arrow.vector.holders.Float8Holder toleranceInput;

  @Output
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryOutput;

  @Inject
  org.apache.arrow.memory.ArrowBuf buffer;

  public void setup() {
  }

  public void eval() {
    if (org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.isHolderSet(binaryInput)) {
      double tolerance = toleranceInput.value;
      org.locationtech.jts.geom.Geometry geom = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toGeometry(binaryInput);
      org.locationtech.jts.simplify.DouglasPeuckerSimplifier simplifier = new org.locationtech.jts.simplify.DouglasPeuckerSimplifier(geom);
      simplifier.setDistanceTolerance(tolerance);
      org.locationtech.jts.geom.Geometry simplified = simplifier.getResultGeometry();
      simplified.setSRID(geom.getSRID());
      byte[] bytes = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toEWKB(simplified);
      buffer = buffer.reallocIfNeeded(bytes.length);
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.populate(bytes, buffer, binaryOutput);
    } else {
      org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.markHolderNotSet(binaryOutput);
    }
  }
}