package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STRelateTests : GeometryRelationFunSpec.NullableVarCharOutput<STRelate>() {

  override val function = STRelate().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    matrixOutput = NullableVarCharHolder()
    buffer = allocateBuffer()
  }

  init {

    testGeometryRelation(
      "Calling ST_Relate on 2 given relating LINESTRINGs returns the given intersection matrix",
      "LINESTRING(0 1,2 2)",
      "LINESTRING(2 2,0 1)",
      "1FFF0FFF2"
    )

    testGeometryRelation(
      "Calling ST_Relate on the given POINT and POLYGON returns the given intersection matrix",
      "POINT(0 0)",
      "LINESTRING(1 5,0 1)",
      "FF0FFF102"
    )
  }

  override val STRelate.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STRelate.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STRelate.output: NullableVarCharHolder get() = function.matrixOutput
}