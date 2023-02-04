package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STDWithinTests : GeometryRelationFunSpec.NullableBitOutput<STDWithin>() {

  override val function = STDWithin().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    distanceInput = Float8Holder()
    output = NullableBitHolder()
  }

  init {

    beforeEach {
      function.distanceInput.reset()
    }

    testTrueGeometryRelation(
      "Calling ST_DWithin with a distance of 2.0 on 2 given relating LINESTRINGs",
      "LINESTRING(0 1,2 2)",
      "LINESTRING(2 2,0 1)"
    ) { function.apply { distanceInput.value = 2.0 } }

    testFalseGeometryRelation(
      "Calling ST_DWithin with a distance of 0.02 on the given POINT and LINESTRING",
      "POINT(0 0)",
      "LINESTRING(1 5,0 1)"
    ) { function.apply { distanceInput.value = 0.02 } }

    testNullGeometryRelation(
      "Calling ST_DWithin with one or two null geometries",
      "POINT(0 0)",
      null,
    ) { function.apply { distanceInput.value = 0.02 } }
  }

  override val STDWithin.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STDWithin.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STDWithin.output: NullableBitHolder get() = function.output
}
