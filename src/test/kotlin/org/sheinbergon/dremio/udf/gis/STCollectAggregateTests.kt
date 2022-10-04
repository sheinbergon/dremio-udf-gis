package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STCollectAggregateTests : GeometryAggregationFunSpec<STCollectAggregate>() {

  init {
    testGeometryAggegration(
      "Calling ST_Collect on several POLYGONs returns a MULTIPOLYGON",
      arrayOf(
        "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))",
        "POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))",
        "POLYGON((50 40, 20 30, 10 20, 50 40))",
        "POLYGON EMPTY"
      ),
      "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1)), ((30 10, 40 40, 20 40, 10 20, 30 10)), ((50 40, 20 30, 10 20, 50 40)))"
    )

    testGeometryAggegrationNoInput("Calling ST_Collect on no/null input returns nothing")
  }

  override val function = STCollectAggregate().apply {
    value = NullableVarBinaryHolder()
    input = NullableVarBinaryHolder()
    indicator = BitHolder()
    output = NullableVarBinaryHolder()
    valueBuffer = allocateBuffer()
    outputBuffer = allocateBuffer()
  }

  override val STCollectAggregate.wkbInput: NullableVarBinaryHolder get() = function.input
  override val STCollectAggregate.wkbOutput: NullableVarBinaryHolder get() = function.output
  override val STCollectAggregate.aggregationValue: NullableVarBinaryHolder get() = function.value
  override val STCollectAggregate.setIndicator: BitHolder get() = function.indicator
}
