package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAggregationFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STCollectAggregateTests : GeometryAggregationFunSpec<STCollectAggregate>() {

  init {
    testGeometryAggregration(
      "Calling ST_Collect on several POLYGONs returns a MULTIPOLYGON",
      arrayOf(
        "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))",
        "POLYGON((50 40, 20 30, 10 20, 50 40))",
        "POLYGON EMPTY"
      ),
      "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1)), ((50 40, 20 30, 10 20, 50 40)), EMPTY)"
    )

    testGeometryAggregationNoInput("Calling ST_Collect on no/null input returns nothing")
  }

  override val function = STCollectAggregate().apply {
    value = NullableVarBinaryHolder()
    input = NullableVarBinaryHolder()
    indicator = NullableBitHolder()
    output = NullableVarBinaryHolder()
    valueBuffer = allocateBuffer()
    outputBuffer = allocateBuffer()
  }

  override val STCollectAggregate.wkbInput: NullableVarBinaryHolder get() = function.input
  override val STCollectAggregate.wkbOutput: NullableVarBinaryHolder get() = function.output
  override val STCollectAggregate.aggregationValue: NullableVarBinaryHolder get() = function.value
  override val STCollectAggregate.setIndicator: NullableBitHolder get() = function.indicator
}
