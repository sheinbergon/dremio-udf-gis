package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec

internal class STDisjointTests : GeometryRelationFunSpec.BitOutput<STDisjoint>() {

  init {
    testFalseGeometryRelation(
      "Calling ST_Disjoint on a POINT within a POLYGON",
      "POINT(0.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Disjoint on a POINT outside of a POLYGON",
      "POINT(22.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Disjoint on a POINT touching a POLYGON",
      "POINT(0.0 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Disjoint on a LINESTRING disjoint with a POLYGON",
      "LINESTRING(2.0 5.0,9.0 2.0)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )
  }

  override val function = STDisjoint().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = BitHolder()
  }

  override val STDisjoint.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STDisjoint.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STDisjoint.output: BitHolder get() = function.output
}
