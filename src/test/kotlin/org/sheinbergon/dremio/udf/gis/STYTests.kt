package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STYTests : GeometryAccessorFunSpec<STY, NullableFloat8Holder>() {

  init {
    testGeometryAccessor(
      "Calling ST_Y on a POINT geometry returns its coordinate's Y value",
      "POINT(1.92 345.214)",
      345.214
    )

    testNoResultGeometryAccessor(
      "Calling ST_Y on a non-POINT geometry returns 0",
      "LINESTRING(1.8 345.2, 1.9 359.2, 2.0 360.0)"
    )
  }

  override val function = STY().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }
  override val STY.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STY.output: NullableFloat8Holder get() = function.output
}
