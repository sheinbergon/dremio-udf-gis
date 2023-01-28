package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STSimplifyTests : GeometryProcessingFunSpec<STSimplify>() {

  init {

    beforeEach {
      function.toleranceInput.reset()
    }

    testGeometryProcessing(
      name = "Calling ST_Simplify on a LINESTRING with a tolerance of 1000",
      wkt = "LINESTRING(-122.306067 37.55412,-122.32328 37.561801,-122.325879 37.586852)",
      expected = "LINESTRING (-122.30607 37.55412, -122.32588 37.58685)"
    ) { function.toleranceInput.value = 1000.0 }

    testNullGeometryProcessing(
      "Calling ST_Simplify on a NULL input"
    )
  }

  override val function = STSimplify().apply {
    binaryInput = NullableVarBinaryHolder()
    toleranceInput = Float8Holder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STSimplify.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STSimplify.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
