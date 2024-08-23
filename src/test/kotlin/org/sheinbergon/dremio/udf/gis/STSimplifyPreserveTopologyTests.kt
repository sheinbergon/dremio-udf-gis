package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STSimplifyPreserveTopologyTests : GeometryProcessingFunSpec<STSimplifyPreserveTopology>() {

  init {

    beforeEach {
      function.toleranceInput.reset()
    }

    testGeometryProcessing(
      name = "Calling ST_SimplifyPreserveTopology on a MULTILINESTRING with a tolerance of 40.0",
      wkt = """
        MULTILINESTRING (
          (20 180, 20 150, 50 150, 50 100, 110 150, 150 140, 170 120), 
          (20 10, 80 30, 90 120), 
          (90 120, 130 130), 
          (130 130, 130 70, 160 40, 180 60, 180 90, 140 80), 
          (50 40, 70 40, 80 70, 70 60, 60 60, 50 50, 50 40) 
      )""".trimIndent(),
      expected = """
        MULTILINESTRING(
          (20 180,50 100,110 150,170 120),
          (20 10,90 120),
          (90 120,130 130),
          (130 130,130 70,160 40,180 90,140 80),
          (50 40,70 40,80 70,60 60,50 40)
        )""".trimIndent()
    ) { function.toleranceInput.value = 40.0 }

    testNullGeometryProcessing(
      "Calling ST_SimplifyPreserveTopology on a NULL input"
    )
  }

  override val function = STSimplifyPreserveTopology().apply {
    binaryInput = NullableVarBinaryHolder()
    toleranceInput = Float8Holder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STSimplifyPreserveTopology.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STSimplifyPreserveTopology.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
