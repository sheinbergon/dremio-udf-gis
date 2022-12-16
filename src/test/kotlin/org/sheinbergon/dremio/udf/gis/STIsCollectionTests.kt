package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STIsCollectionTests : GeometryAccessorFunSpec<STIsCollection, NullableBitHolder>() {

  init {
    testGeometryAccessor(
      "Calling ST_IsCollection on a POINT geometry returns false",
      "POINT(1.92 345.214)",
      false
    )

    testGeometryAccessor(
      "Calling ST_IsCollection on a MULTIPOINT geometry returns true",
      "MULTIPOINT((0 0), (42 42))",
      true
    )

    testNullGeometryAccessor(
      "Calling ST_IsCollection on null input returns false",
    )
  }

  override val function = STIsCollection().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableBitHolder()
  }

  override val STIsCollection.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STIsCollection.output: NullableBitHolder get() = function.output
}
