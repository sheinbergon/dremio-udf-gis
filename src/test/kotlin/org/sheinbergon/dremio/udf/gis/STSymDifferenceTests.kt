package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOperatorsFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STSymDifferenceTests : GeometryOperatorsFunSpec<STSymDifference>() {

  init {
    testGeometryOperator(
      "Calling ST_SymDifference on non-intersecting POINT and LINESTRING returns a geometry collection containing both of them",
      "POINT (-1 -1)",
      "LINESTRING (2 0, 0 2)",
      "GEOMETRYCOLLECTION (POINT (-1 -1), LINESTRING (2 0, 0 2))"
    )

    testGeometryOperator(
      "Calling ST_SymDifference on 2 intersecting LINESTRINGs returns their respective symmetric difference",
      "LINESTRING(50 100, 50 200)",
      "LINESTRING(50 50, 50 150)",
      "MULTILINESTRING ((50 150, 50 200), (50 50, 50 100))"
    )
  }

  override val function = STSymDifference().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STSymDifference.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STSymDifference.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STSymDifference.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
