package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec

internal class STCrossesTests : GeometryRelationFunSpec.NullableBitOutput<STCrosses>() {

  init {
    testFalseGeometryRelation(
      "Calling ST_Crosses on a POINT within a POLYGON",
      "POINT(0.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Crosses on a POLYGON containing a POINT",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))",
      "POINT(0.5 0.5)"
    )

    testFalseGeometryRelation(
      "Calling ST_Crosses on a POINT outside of a POLYGON",
      "POINT(22.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Crosses on a POINT touching a POLYGON",
      "POINT(0.0 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Crosses on a LINESTRING intersecting with a POLYGON",
      "LINESTRING(2.0 0.0,0.0 1.0)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Crosses on a LINESTRING crossing a POLYGON",
      "LINESTRING(2.0 0.5,0.1 0.1)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Crosses on a POLYGON containing a LINESTRING",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))",
      "LINESTRING(0.4 0.5,0.7 0.6)"
    )

    testNullGeometryRelation(
      "Calling ST_Crosses with one or two null geometries",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))",
      null,
    )
  }

  override val function = STCrosses().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = NullableBitHolder()
  }

  override val STCrosses.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STCrosses.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STCrosses.output: NullableBitHolder get() = function.output
}
