package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOperatorsFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STIntersectionTests : GeometryOperatorsFunSpec<STIntersection>() {

  init {
    testGeometryOperator(
      "Calling ST_Intersection on non-intersecting POINT and LINESTRING returns an empty geometry",
      "POINT (0 0)",
      "LINESTRING (2 0, 0 2)",
      "POINT EMPTY"
    )

    testGeometryOperator(
      "Calling ST_Intersection on 2 intersecting POLYGONs returns their intersection",
      "POLYGON((1 2,1 5,4 5,4 2,1 2))",
      "POLYGON((3 1,3 3,5 3,5 1,3 1))",
      "POLYGON ((4 3, 4 2, 3 2, 3 3, 4 3))"
    )
  }

  override val function = STIntersection().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STIntersection.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STIntersection.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STIntersection.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
