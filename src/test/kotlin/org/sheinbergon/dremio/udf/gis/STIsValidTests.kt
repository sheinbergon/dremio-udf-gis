package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STIsValidTests : GeometryAccessorFunSpec<STIsValid, NullableBitHolder>() {

  init {
    testGeometryAccessor(
      "Calling ST_IsValid on a self-intersecting Polygon",
      "POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))",
      false
    )

    testGeometryAccessor(
      "Calling ST_IsValid on a valid LineString",
      "LINESTRING(0 0, 1 1)",
      true
    )

    testNullGeometryAccessor(
      "Calling ST_IsValid on null input returns false",
    )
  }

  override val function = STIsValid().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableBitHolder()
  }

  override val STIsValid.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STIsValid.output: NullableBitHolder get() = function.output
}
