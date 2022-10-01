package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec

internal class STTouchesTests : GeometryRelationFunSpec.BitOutput<STTouches>() {

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
  }

  override val function = STTouches().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = BitHolder()
  }

  override val STTouches.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STTouches.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STTouches.output: BitHolder get() = function.output
}
