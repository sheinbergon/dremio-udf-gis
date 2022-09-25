package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STYMinTests : GeometryAccessorFunSpec<STYMin, Float8Holder>() {

  init {

    testGeometryAccesssor(
      "Calling ST_YMax on a POINT geometry returns its coordinate's Y value",
      "POINT(1.92 300.122)",
      300.122
    )

    testGeometryAccesssor(
      "Calling ST_YMax on a LINESTRING geometry returns its minimal Y value",
      "LINESTRING(1.8 345.2, 1.9 359.2, 2.0 360.0)",
      345.2
    )
  }

  override val function = STYMin().apply {
    binaryInput = NullableVarBinaryHolder()
    output = Float8Holder()
  }
  override val STYMin.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STYMin.output: Float8Holder get() = function.output
}