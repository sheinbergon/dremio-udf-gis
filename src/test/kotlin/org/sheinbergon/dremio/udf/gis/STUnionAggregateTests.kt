package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAggregationFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STUnionAggregateTests : GeometryAggregationFunSpec<STUnionAggregate>() {

  init {
    testGeometryAggegration(
      "Calling ST_UNION on several GEOMETRY types returns their union",
      arrayOf(
        "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))",
        "POLYGON((1 2, 1 9, 3 5, 1 2))",
        "POINT(1 1)",
        "GEOMETRYCOLLECTION EMPTY"
      ),
      "POLYGON ((2.33333 4, 4 4, 4 0, 0 0, 0 4, 1 4, 1 9, 3 5, 2.33333 4))"
    )

    testGeometryAggegrationNoInput("Calling ST_UNION on no/null input returns nothing")
  }

  override val function = STUnionAggregate().apply {
    value = NullableVarBinaryHolder()
    input = NullableVarBinaryHolder()
    indicator = NullableBitHolder()
    output = NullableVarBinaryHolder()
    valueBuffer = allocateBuffer()
    outputBuffer = allocateBuffer()
  }

  override val STUnionAggregate.wkbInput: NullableVarBinaryHolder get() = function.input
  override val STUnionAggregate.wkbOutput: NullableVarBinaryHolder get() = function.output
  override val STUnionAggregate.aggregationValue: NullableVarBinaryHolder get() = function.value
  override val STUnionAggregate.setIndicator: NullableBitHolder get() = function.indicator
}
