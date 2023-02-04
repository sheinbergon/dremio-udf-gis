package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec

internal class STTouchesTests : GeometryRelationFunSpec.NullableBitOutput<STTouches>() {

  init {
    testFalseGeometryRelation(
      "Calling ST_Touches on a POINT within a POLYGON",
      "POINT(0.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testFalseGeometryRelation(
      "Calling ST_Touches on a POINT outside of a POLYGON",
      "POINT(22.5 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Touches on a POINT touching a POLYGON",
      "POINT(0.0 0.5)",
      "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
    )

    testTrueGeometryRelation(
      "Calling ST_Touches on a 2 LINESTRINGs touching each other",
      "LINESTRING(2.0 0.0,1.0 1.0)",
      "LINESTRING(0.0 0.0,1.0 0.0,1.0 1.0)"
    )

    testNullGeometryRelation(
      "Calling ST_Touches with one or two null geometries",
      null,
      "LINESTRING(-0.5 0.5,0.5 0.5)"
    )

    testDifferentSRIDGeometryRelation(
      "Calling ST_Touches on geometries specified using different SRID",
      "LINESTRING(-0.5 0.5,0.5 0.5)",
      "POLYGON((0 0,111319.49079327357 0,111319.49079327357 111325.14286638486,0 111325.14286638486,0 0))",
      4326,
      3857
    )
  }

  override val function = STTouches().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = NullableBitHolder()
  }

  override val STTouches.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STTouches.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STTouches.output: NullableBitHolder get() = function.output
}
