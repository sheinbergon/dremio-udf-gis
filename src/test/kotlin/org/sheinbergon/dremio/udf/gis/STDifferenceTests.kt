package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOperatorsFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STDifferenceTests : GeometryOperatorsFunSpec<STDifference>() {

  init {
    testGeometryOperator(
      "Calling ST_Difference on non-intersecting POINT and LINESTRING returns the POINT",
      "POINT (-1 -1)",
      "LINESTRING (2 0, 0 2)",
      "POINT (-1 -1)"
    )

    testGeometryOperator(
      "Calling ST_Difference on 2 intersecting POLYGONs returns their difference",
      "POLYGON((1 2,1 5,4 5,4 2,1 2))",
      "POLYGON((3 1,3 3,5 3,5 1,3 1))",
      "POLYGON((1 2,1 5,4 5,4 3,3 3,3 2,1 2))"
    )
  }

  override val function = STDifference().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STDifference.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STDifference.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STDifference.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
