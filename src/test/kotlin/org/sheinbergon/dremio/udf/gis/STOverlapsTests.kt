package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec

internal class STOverlapsTests : GeometryRelationFunSpec.NullableBitOutput<STOverlaps>() {

  init {
    testFalseGeometryRelation(
      "Calling ST_Overlaps on a POINT within a POLYGON",
      "POINT(0.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Overlaps on a POLYGON containing a POINT",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))",
      "POINT(0.5 0.5)"
    )

    testFalseGeometryRelation(
      "Calling ST_Overlaps on a POINT outside of a POLYGON",
      "POINT(22.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Overlaps on a POINT touching a POLYGON",
      "POINT(0.0 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Overlaps on a LINESTRING intersecting with a POLYGON",
      "LINESTRING(2.0 0.0,0.0 1.0)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Overlaps on 2 overlapping LINESTRINGSs",
      "LINESTRING(3.0 0.7,2.0 0.5,-2.0 0.6,-3.0 0.7)",
      "LINESTRING(0.5 0.0,2.0 0.5,-2.0 0.6,-2.5 0.3)"
    )

    testTrueGeometryRelation(
      "Calling ST_Overlaps on 2 overlapping POLYGONs",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))",
      "POLYGON((0.9 0.0,0.9 1.0,1.5 1.0,1.5 0.0,0.9 0.0))",
    )
  }

  override val function = STOverlaps().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = NullableBitHolder()
  }

  override val STOverlaps.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STOverlaps.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STOverlaps.output: NullableBitHolder get() = function.output
}
