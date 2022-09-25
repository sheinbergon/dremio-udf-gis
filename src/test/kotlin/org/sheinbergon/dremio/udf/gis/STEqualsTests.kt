package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec

internal class STEqualsTests : GeometryRelationFunSpec.BitOutput<STEquals>() {

  init {
    testFalseGeometryRelation(
      "Calling ST_Equals on a POINT and a POLYGON",
      "POINT(0.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Equals on 2 identical POINTs",
      "POINT(1 1)",
      "POINT(1.0 1.0)"
    )

    testFalseGeometryRelation(
      "Calling ST_Equals on a LINESTRING and a POLYGON",
      "LINESTRING(2.0 0.0,0.0 1.0)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Equals on 2 inverse LINESTRINGs",
      "LINESTRING(3.0 0.7,2.0 0.5,-2.0 0.6,-3.0 0.7)",
      "LINESTRING(-3.0 0.7,-2.0 0.6,2.0 0.5,3.0 0.7)"
    )

    testTrueGeometryRelation(
      "Calling ST_Equals on 2 identical POLYGONs",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )
  }

  override val function = STEquals().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = BitHolder()
  }

  override val STEquals.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STEquals.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STEquals.output: BitHolder get() = function.output
}