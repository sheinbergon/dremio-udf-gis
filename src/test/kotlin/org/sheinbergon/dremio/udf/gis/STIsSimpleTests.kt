package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STIsSimpleTests : GeometryAccessorFunSpec<STIsSimple, NullableBitHolder>() {

  init {
    testGeometryAccessor(
      "Calling ST_IsSimple on a self-intersecting Polygon",
      "POLYGON((1 2, 3 4, 5 6, 1 2))",
      false
    )

    testGeometryAccessor(
      "Calling ST_IsSimple on a simple LineString",
      "LINESTRING(2 1, 1 3, 6 6, 5 7, 5 6)",
      true
    )

    testNullGeometryAccessor(
      "Calling ST_IsSimple on null input returns false",
    )
  }

  override val function = STIsSimple().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableBitHolder()
  }

  override val STIsSimple.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STIsSimple.output: NullableBitHolder get() = function.output
}
