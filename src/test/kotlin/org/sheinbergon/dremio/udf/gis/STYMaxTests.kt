package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STYMaxTests : GeometryAccessorFunSpec<STYMax, NullableFloat8Holder>() {

  init {
    testGeometryAccessor(
      "Calling ST_YMax on a POINT geometry returns its coordinate's Y value",
      "POINT(1.92 355.921)",
      355.921
    )

    testGeometryAccessor(
      "Calling ST_YMax on a LINESTRING geometry returns its maximal Y value",
      "LINESTRING(1.8 345.2, 1.9 359.2, 2.0 360.0)",
      360.0
    )
  }

  override val function = STYMax().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }
  override val STYMax.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STYMax.output: NullableFloat8Holder get() = function.output
}
