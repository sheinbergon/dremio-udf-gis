package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOverlayFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STUnionTests : GeometryOverlayFunSpec<STUnion>() {

  init {
    testGeometryOverlay(
      "Calling ST_Union on different POINTs returns their unionas a MULTIPOINT collection",
      "POINT (1 2)",
      "POINT (-2 3)",
      "MULTIPOINT(-2 3,1 2)"
    )

    testGeometryOverlay(
      "Calling ST_Union on the same POINT returns, well, that very same POINT",
      "POINT (1 2)",
      "POINT (1 2)",
      "POINT (1 2)"
    )
  }

  override val function = STUnion().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STUnion.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STUnion.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STUnion.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
