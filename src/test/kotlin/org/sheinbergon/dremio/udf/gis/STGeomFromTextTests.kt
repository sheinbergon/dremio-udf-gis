package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryInputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STGeomFromTextTests : GeometryInputFunSpec.NullableVarChar<STGeomFromText>() {

  init {
    testGeometryInput(
      "Calling ST_GeomFromText on a POINT",
      "POINT(0.5 0.5)",
      byteArrayOf(1, 1, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63)
    )

    testInvalidGeometryInput(
      "Calling ST_GeomFromText on rubbish text",
      "42ifon2 fA!@",
    )

    testNullGeometryInput(
      "Calling ST_GeomFromText on null input"
    )
  }

  override val function = STGeomFromText().apply {
    wktInput = NullableVarCharHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STGeomFromText.input: NullableVarCharHolder get() = function.wktInput
  override val STGeomFromText.output: NullableVarBinaryHolder get() = function.binaryOutput
}
