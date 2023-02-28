package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableIntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STNumGeometriesTests : GeometryAccessorFunSpec<STNumGeometries, NullableIntHolder>() {

  init {
    testGeometryAccessor(
      "Calling ST_NumGeometries on a POINT geometry returns 1",
      "POINT(1.92 345.214)",
      1
    )

    testGeometryAccessor(
      "Calling ST_NumGeometries on a MULTIPOLYGON geometry returns the num of polygons in the collection",
      "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))",
      2
    )

    testNullGeometryAccessor(
      "Calling ST_NumGeometries on null input",
    )
  }

  override val function = STNumGeometries().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableIntHolder()
  }

  override val STNumGeometries.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STNumGeometries.output: NullableIntHolder get() = function.output
}
