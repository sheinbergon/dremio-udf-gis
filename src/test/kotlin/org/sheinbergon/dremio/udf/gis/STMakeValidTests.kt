package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STMakeValidTests : GeometryProcessingFunSpec<STMakeValid>() {

  init {
    testGeometryProcessing(
      name = "Calling ST_MakeValid on a POLYGON with a self-intersecting ring",
      wkt = "POLYGON((5 0, 10 0, 10 10, 0 10, 0 0, 5 0, 3 3, 5 6, 7 3, 5 0))",
      expected = "POLYGON ((5 0, 0 0, 0 10, 10 10, 10 0, 5 0), (5 0, 7 3, 5 6, 3 3, 5 0))"
    )

    testGeometryProcessing(
      name = "Calling ST_MakeValid on a POLYGON with outside holes",
      wkt = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))",
      expected = "MULTIPOLYGON (((0 10, 10 10, 10 0, 0 0, 0 10)), ((15 20, 20 20, 20 15, 15 15, 15 20)))"
    )

    testGeometryProcessing(
      name = "Calling ST_MakeValid on a MULTIPOLYGON with duplicated rings",
      wkt = "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)),((0 0, 10 0, 10 10, 0 10, 0 0)))",
      expected = "MULTIPOLYGON (((0 10, 10 10, 10 0, 0 0, 0 10)))"
    )

    testGeometryProcessing(
      name = "Calling ST_MakeValid on a zero-length LINESTRING",
      wkt = "LINESTRING (0 0, 0 0)",
      expected = "LINESTRING EMPTY"
    )

    testNullGeometryProcessing(
      "Calling ST_MakeValid on a NULL input"
    )
  }

  override val function = STMakeValid().apply {
    binaryInput = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STMakeValid.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STMakeValid.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
