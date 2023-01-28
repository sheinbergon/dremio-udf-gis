package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOutputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STAsTextTests : GeometryOutputFunSpec.NullableVarChar<STAsText>() {

  init {
    testGeometryOutput(
      "Calling ST_AsText on a POINT",
      "POINT(0.5 0.5)",
      4326,
      "POINT (0.5 0.5)"
    )

    testGeometryOutput(
      "Calling ST_AsText on a POLYGON",
      "POLYGON((0.0 0.0,1.23 0.0,1.0 1.0,0.19 1.0,0.0 0.0))",
      3857,
      "POLYGON ((0 0, 1.23 0, 1 1, 0.19 1, 0 0))"
    )

    testNullGeometryOutput(
      "Calling ST_AsText on null input",
    )
  }

  override val function = STAsText().apply {
    binaryInput = NullableVarBinaryHolder()
    wktOutput = NullableVarCharHolder()
    buffer = allocateBuffer()
  }

  override val STAsText.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STAsText.output: NullableVarCharHolder get() = function.wktOutput
}
