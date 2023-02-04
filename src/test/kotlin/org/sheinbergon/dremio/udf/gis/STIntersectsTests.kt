package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec

internal class STIntersectsTests : GeometryRelationFunSpec.NullableBitOutput<STIntersects>() {

  init {
    testTrueGeometryRelation(
      "Calling ST_Intersects on a POINT within a POLYGON",
      "POINT(0.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Intersects on a POINT outside of a POLYGON",
      "POINT(22.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Intersects on a POINT touching a POLYGON",
      "POINT(0.0 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Intersects on a LINESTRING intersecting with a POLYGON",
      "LINESTRING(2.0 0.0,0.0 1.0)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Intersects on a LINESTRING crossing a POLYGON",
      "LINESTRING(2.0 0.5,-2.0 0.6)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testNullGeometryRelation(
      "Calling ST_Intersects with one or two null geometries",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))",
      null,
    )

    testDifferentSRIDGeometryRelation(
      "Calling ST_Intersects on geometries specified using different SRID",
      "POINT(0 0)",
      "POLYGON((0 0,111319.49079327357 0,111319.49079327357 111325.14286638486,0 111325.14286638486,0 0))",
      null,
      3857
    )
  }

  override val function = STIntersects().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = NullableBitHolder()
  }

  override val STIntersects.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STIntersects.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STIntersects.output: NullableBitHolder get() = function.output
}
