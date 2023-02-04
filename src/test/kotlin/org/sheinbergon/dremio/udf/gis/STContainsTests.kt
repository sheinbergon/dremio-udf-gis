package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec

internal class STContainsTests : GeometryRelationFunSpec.NullableBitOutput<STContains>() {

  init {
    testFalseGeometryRelation(
      "Calling ST_Contains on a POINT within a POLYGON",
      "POINT(0.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Contains on a POLYGON containing a POINT",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))",
      "POINT(0.5 0.5)"
    )

    testFalseGeometryRelation(
      "Calling ST_Contains on a POINT outside of a POLYGON",
      "POINT(22.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Contains on a POINT touching a POLYGON",
      "POINT(0.0 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Contains on a LINESTRING intersecting with a POLYGON",
      "LINESTRING(2.0 0.0,0.0 1.0)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Contains on a LINESTRING crossing a POLYGON",
      "LINESTRING(2.0 0.5,-2.0 0.6)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Contains on a POLYGON containing a LINESTRING",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))",
      "LINESTRING(0.4 0.5,0.7 0.6)"
    )

    testNullGeometryRelation(
      "Calling ST_Contains with one or two null geometries",
      null,
      "LINESTRING(0.4 0.5,0.7 0.6)"
    )

    testDifferentSRIDGeometryRelation(
      "Calling ST_Contains on geometries specified using different SRID",
      "LINESTRING(2.0 0.5,-2.0 0.6)",
      "LINESTRING(0.4 0.5,0.7 0.6)",
      4326,
    )
  }

  override val function = STContains().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = NullableBitHolder()
  }

  override val STContains.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STContains.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STContains.output: NullableBitHolder get() = function.output
}
