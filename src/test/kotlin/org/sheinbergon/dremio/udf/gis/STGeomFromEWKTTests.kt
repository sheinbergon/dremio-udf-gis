package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryInputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STGeomFromEWKTTests : GeometryInputFunSpec.NullableVarChar<STGeomFromEWKT>() {

  init {
    testGeometryInput(
      "Calling ST_GeomFromEWKT on an EWKT POINT",
      "SRID=4326;POINT(0.5 0.5)",
      byteArrayOf(1, 1, 0, 0, 32, -26, 16, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63)
    )

    testInvalidGeometryInput(
      "Calling ST_GeomFromEWKT on WKT",
      "POINT(0.5 0.5)",
    )

    testNullGeometryInput(
      "Calling ST_GeomFromEWKT on null input"
    )
  }

  override val function = STGeomFromEWKT().apply {
    ewktInput = NullableVarCharHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STGeomFromEWKT.input: NullableVarCharHolder get() = function.ewktInput
  override val STGeomFromEWKT.output: NullableVarBinaryHolder get() = function.binaryOutput
}
