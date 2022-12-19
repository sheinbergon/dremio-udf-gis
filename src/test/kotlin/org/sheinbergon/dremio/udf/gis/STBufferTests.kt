package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STBufferTests : GeometryProcessingFunSpec<STBuffer>() {

  init {

    beforeEach {
      function.radiusInput.reset()
    }

    testGeometryProcessing(
      name = "Calling ST_Buffer on a POINT",
      wkt = "POINT(0 0)",
      expected = """
        POLYGON (
          (
            1 0, 
            0.98079 -0.19509, 
            0.92388 -0.38268, 
            0.83147 -0.55557, 
            0.70711 -0.70711, 
            0.55557 -0.83147, 
            0.38268 -0.92388, 
            0.19509 -0.98079, 
            0 -1, 
            -0.19509 -0.98079, 
            -0.38268 -0.92388, 
            -0.55557 -0.83147, 
            -0.70711 -0.70711, 
            -0.83147 -0.55557, 
            -0.92388 -0.38268, 
            -0.98079 -0.19509, 
            -1 0, 
            -0.98079 0.19509, 
            -0.92388 0.38268, 
            -0.83147 0.55557, 
            -0.70711 0.70711, 
            -0.55557 0.83147, 
            -0.38268 0.92388, 
            -0.19509 0.98079, 
            0 1, 
            0.19509 0.98079, 
            0.38268 0.92388, 
            0.55557 0.83147, 
            0.70711 0.70711, 
            0.83147 0.55557, 
            0.92388 0.38268, 
            0.98079 0.19509, 
            1 0))
      """.trimIndent()
    ) { function.radiusInput.value = 1.0 }

    testNullGeometryProcessing(
      "Calling ST_Buffer on a NULL input"
    )
  }

  override val function = STBuffer().apply {
    binaryInput = NullableVarBinaryHolder()
    radiusInput = Float8Holder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STBuffer.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STBuffer.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
