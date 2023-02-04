package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setUtf8

internal class STRelateMatrixTests : GeometryRelationFunSpec.NullableBitOutput<STRelateMatrix>() {

  override val function = STRelateMatrix().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    matrixInput = NullableVarCharHolder()
    output = NullableBitHolder()
  }

  init {

    beforeEach {
      function.matrixInput.reset()
    }

    testTrueGeometryRelation(
      "Calling ST_Relate with a matrix param on 2 given relating LINESTRINGs",
      "LINESTRING(0 1,2 2)",
      "LINESTRING(2 2,0 1)"
    ) { function.apply { matrixInput.setUtf8("T*F**FFF2") } }

    testFalseGeometryRelation(
      "Calling ST_Relate with a matrix param on the given POINT and LINESTRING",
      "POINT(0 0)",
      "LINESTRING(1 5,0 1)"
    ) { function.apply { matrixInput.setUtf8("T*T**FFF0") } }

    testNullGeometryRelation(
      "Calling ST_Relate with one or two null geometries",
      "LINESTRING(-0.5 0.5,0.5 0.5)",
      null,
    ) { function.apply { matrixInput.setUtf8("T*T**FFF0") } }
  }

  override val STRelateMatrix.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STRelateMatrix.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STRelateMatrix.output: NullableBitHolder get() = function.output
}
