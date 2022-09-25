package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STXTests : GeometryAccessorFunSpec<STX, NullableFloat8Holder>() {

  init {
    testGeometryAccesssor(
      "Calling ST_X on a POINT geometry returns its coordinate's X value",
      "POINT(1.92 345.214)",
      1.92
    )

    testNoResultGeometryAccessor(
      "Calling ST_X on a non-POINT geometry returns 0",
      "LINESTRING(1.8 345.2, 1.9 359.2, 2.0 360.0)"
    )
  }

  override val function = STX().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }
  override val STX.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STX.output: NullableFloat8Holder get() = function.output
}