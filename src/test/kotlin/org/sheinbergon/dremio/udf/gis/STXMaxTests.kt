package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STXMaxTests : GeometryAccessorFunSpec<STXMax, Float8Holder>() {

  init {
    testGeometryAccesssor(
      "Calling ST_XMax on a POINT geometry returns its coordinate's X value",
      "POINT(1.92 345.214)",
      1.92
    )

    testGeometryAccesssor(
      "Calling ST_XMax on a LINESTRING geometry returns its maximal X value",
      "LINESTRING(1.8 345.2, 1.9 359.2, 2.0 360.0)",
      2.0
    )
  }

  override val function = STXMax().apply {
    binaryInput = NullableVarBinaryHolder()
    output = Float8Holder()
  }
  override val STXMax.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STXMax.output: Float8Holder get() = function.output
}