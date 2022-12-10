package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOutputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STAsEWKTTests : GeometryOutputFunSpec<STAsEWKT>() {

  init {
    testGeometryOutput(
      "Calling ST_AsEWKT on a POINT",
      "POINT(0.5 0.5)",
      4326,
      "SRID=4326;POINT (0.5 0.5)"
    )

    testGeometryOutput(
      "Calling ST_AsEWKT on a POLYGON",
      "POLYGON((0.0 0.0,1.23 0.0,1.0 1.0,0.19 1.0,0.0 0.0))",
      3857,
      "SRID=3857;POLYGON ((0 0, 1.23 0, 1 1, 0.19 1, 0 0))"
    )
  }

  override val function = STAsEWKT().apply {
    binaryInput = NullableVarBinaryHolder()
    ewktOutput = NullableVarCharHolder()
    buffer = allocateBuffer()
  }

  override val STAsEWKT.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STAsEWKT.output: NullableVarCharHolder get() = function.ewktOutput
}
