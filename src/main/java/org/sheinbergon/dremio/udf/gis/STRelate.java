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
import java.nio.charset.StandardCharsets;

@FunctionTemplate(
    name = "ST_Relate",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STRelate implements SimpleFunction {
  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput1;

  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput2;

  @Output
  org.apache.arrow.vector.holders.NullableVarCharHolder matrixOutput;

  @Inject
  org.apache.arrow.memory.ArrowBuf buffer;

  @Override
  public void setup() {
  }

  @Override
  public void eval() {
    org.locationtech.jts.geom.Geometry geom1 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toGeometry(binaryInput1);
    org.locationtech.jts.geom.Geometry geom2 = org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.toGeometry(binaryInput2);
    byte[] matrix = geom1.relate(geom2).toString().getBytes(StandardCharsets.UTF_8);
    buffer = buffer.reallocIfNeeded(matrix.length);
    org.sheinbergon.dremio.udf.gis.util.GeometryHelpers.populate(matrix, buffer, matrixOutput);
  }
}
