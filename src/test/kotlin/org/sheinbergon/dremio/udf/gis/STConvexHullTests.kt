package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STConvexHullTests : GeometryProcessingFunSpec<STConvexHull>() {

  init {
    testGeometryProcessing(
      "Calling STConvexHull on a geometry collection",
      "GEOMETRYCOLLECTION(MULTILINESTRING((100 190,10 8),(150 10, 20 30)),MULTIPOINT(50 5, 150 30, 50 10, 10 10))",
      "POLYGON((50 5,10 8,10 10,100 190,150 30,150 10,50 5))"
    )

    testNullGeometryProcessing(
      "Calling STConvexHull on a NULL input"
    )
  }

  override val function = STConvexHull().apply {
    binaryInput = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STConvexHull.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STConvexHull.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
