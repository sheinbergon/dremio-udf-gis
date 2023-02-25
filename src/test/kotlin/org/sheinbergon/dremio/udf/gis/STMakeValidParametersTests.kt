package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setUtf8

internal class STMakeValidParametersTests : GeometryProcessingFunSpec<STMakeValidParameters>() {

  init {

    beforeEach {
      function.parametersInput.reset()
    }

    testGeometryProcessing(
      name = "Calling ST_MakeValid with parameters on a zero-length LINESTRING and keepcollapsed=true",
      wkt = "LINESTRING (0 0, 0 0)",
      expected = "POINT (0 0)"
    ) {
      function.parametersInput.setUtf8("keepcollapsed=true")
    }

    testGeometryProcessing(
      name = "Calling ST_MakeValid with parameters on a zero-length LINESTRING and keepcollapsed=false",
      wkt = "LINESTRING (0 0, 0 0)",
      expected = "LINESTRING EMPTY"
    ) {
      function.parametersInput.setUtf8("keepcollapsed=false")
    }

    testInvalidArgumentGeometryProcessing(
      name = "Calling ST_MakeValid with an invalid parameter string",
      wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))",
    ) {
      function.parametersInput.setUtf8("some_input=false")
    }

    testNullGeometryProcessing(
      "Calling ST_MakeValid with parameters on a NULL input"
    )
  }

  override val function = STMakeValidParameters().apply {
    binaryInput = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    parametersInput = NullableVarCharHolder()
    buffer = allocateBuffer()
  }

  override val STMakeValidParameters.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STMakeValidParameters.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
