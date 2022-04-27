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
import org.sheinbergon.dremio.udf.gis.util.FunctionHelpersXL;

import javax.inject.Inject;

@FunctionTemplate(
    name = "ST_Buffer",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STBuffer implements SimpleFunction {
  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput;

  @Param(constant = true)
  org.apache.arrow.vector.holders.Float8Holder radiusInput;

  @Output
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryOutput;

  @Inject
  ArrowBuf buffer;

  public void setup() {
  }

  public void eval() {
    com.esri.core.geometry.ogc.OGCGeometry geom1 = FunctionHelpersXL.toGeometry(binaryInput);
    com.esri.core.geometry.ogc.OGCGeometry buffered = geom1.buffer(radiusInput.value);
    byte[] bytes = FunctionHelpersXL.toBinary(buffered);
    buffer = buffer.reallocIfNeeded(bytes.length);
    FunctionHelpersXL.populate(bytes, buffer, binaryOutput);
  }
}
