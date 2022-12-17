package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryInputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STGeomFromWKBTests : GeometryInputFunSpec.NullableVarBinary<STGeomFromWKB>() {

  init {
    testGeometryInput(
      "Calling ST_GeomFromWKB on a POINT WKB representation",
      byteArrayOf(0, 32, 0, 0, 1, 0, 0, 0, 0, 63, -32, 0, 0, 0, 0, 0, 0, 63, -32, 0, 0, 0, 0, 0, 0),
      byteArrayOf(1, 1, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63)
    )

    testInvalidGeometryInput(
      "Calling ST_GeomFromWKB on rubbish data",
      byteArrayOf(-9, 22, 2, 9, 1, 0, 0, 12, 2, 93, -22, 0, 0, 0, 99, 22, 74, 63, -74, 0, 0, -9, -2, 0, 0),
    )

    testNullGeometryInput(
      "Calling ST_GeomFromWKB on null input"
    )
  }

  override val function = STGeomFromWKB().apply {
    wkbInput = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STGeomFromWKB.input: NullableVarBinaryHolder get() = function.wkbInput
  override val STGeomFromWKB.output: NullableVarBinaryHolder get() = function.binaryOutput
}
