package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STXMinTests : GeometryAccessorFunSpec<STXMin, Float8Holder>() {
  init {
    testGeometryAccesssor(
      "Calling ST_XMin on a POINT geometry returns its coordinate's X value",
      "POINT(2.99 345.214)",
      2.99
    )

    testGeometryAccesssor(
      "Calling ST_XMax on a LINESTRING geometry returns its minimal X value",
      "LINESTRING(1.8 345.2, 1.9 359.2, 2.0 360.0)",
      1.8
    )
  }

  override val function = STXMin().apply {
    binaryInput = NullableVarBinaryHolder()
    output = Float8Holder()
  }
  override val STXMin.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STXMin.output: Float8Holder get() = function.output
}