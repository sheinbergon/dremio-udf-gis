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
import com.esri.core.geometry.ogc.OGCPoint;

import javax.annotation.Nullable;

@FunctionTemplate(
    name = "ST_X",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STX implements SimpleFunction {
  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput;

  @Output
  org.apache.arrow.vector.holders.Float8Holder output;

  public void setup() {
  }

  public void eval() {
    var geom1 = FunctionHelpersXL.toGeometry(binaryInput);
    output.value = x(geom1);
  }

  private double x(@Nullable final com.esri.core.geometry.ogc.OGCGeometry geometry) {
    if (FunctionHelpersXL.isAPoint(geometry)) {
      return ((OGCPoint) geometry).X();
    } else {
      return Double.NaN;
    }
  }
}
