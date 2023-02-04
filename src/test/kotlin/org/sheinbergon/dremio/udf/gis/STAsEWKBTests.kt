package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOutputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STAsEWKBTests : GeometryOutputFunSpec.NullableVarBinary<STAsEWKB>() {

  init {
    testGeometryEWKTOutput(
      "Calling ST_AsEWKB on a POINT with SRID 4326",
      "POINT(0.5 0.5)",
      4326,
      "SRID=4326;POINT (0.5 0.5)"
    )

    testGeometryEWKTOutput(
      "Calling ST_AsEWKB on a POLYGON with SRID 0",
      "POLYGON((0.0 0.0,1.23 0.0,1.0 1.0,0.19 1.0,0.0 0.0))",
      0,
      "SRID=0;POLYGON ((0 0, 1.23 0, 1 1, 0.19 1, 0 0))"
    )

    testNullGeometryOutput(
      "Calling ST_AsEWKB on null input",
    )
  }

  override val function = STAsEWKB().apply {
    binaryInput = NullableVarBinaryHolder()
    ewkbOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STAsEWKB.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STAsEWKB.output: NullableVarBinaryHolder get() = function.ewkbOutput
}
