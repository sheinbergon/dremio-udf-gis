package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOverlayFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STCollectTests : GeometryOverlayFunSpec<STCollect>() {

  init {
    testGeometryOverlay(
      "Calling ST_Collector on different POINTs returns a MULTIPOINT collection",
      "POINT (1 2)",
      "POINT (-2 3)",
      "MULTIPOINT(1 2,-2 3)"
    )

    testGeometryOverlay(
      "Calling ST_Collect on the same POINT returns a MULTIPOINT collection",
      "POINT (1 2)",
      "POINT (1 2)",
      "MULTIPOINT(1 2,1 2)"
    )

    testGeometryOverlay(
      "Calling ST_Collect on the a POINT and a LINESTRING returns a GEOMETRY collection",
      "POINT (1 2)",
      "LINESTRING (3 5, 2 9)",
      "GEOMETRYCOLLECTION (POINT (1 2),LINESTRING (3 5, 2 9))"
    )
  }

  override val function = STCollect().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STCollect.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STCollect.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STCollect.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
