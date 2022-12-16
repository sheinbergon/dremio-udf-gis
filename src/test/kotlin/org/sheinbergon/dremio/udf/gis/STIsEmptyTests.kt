package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STIsEmptyTests : GeometryAccessorFunSpec<STIsEmpty, BitHolder>() {

  init {
    testGeometryAccessor(
      "Calling ST_IsEmpty on an empty GEOMETRY COLLECTION true",
      "GEOMETRYCOLLECTION EMPTY",
      true
    )

    testGeometryAccessor(
      "Calling ST_IsEmpty on a non-empty POLYGON geometry returns false",
      "POLYGON((1 2, 3 4, 5 6, 1 2))",
      false
    )

    testNullGeometryAccessor(
      "Calling ST_IsEmpty on null input returns false",
    )
  }

  override val function = STIsEmpty().apply {
    binaryInput = NullableVarBinaryHolder()
    output = BitHolder()
  }

  override val STIsEmpty.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STIsEmpty.output: BitHolder get() = function.output
}
