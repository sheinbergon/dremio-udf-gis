package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryInputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STGeomFromEWKBTests : GeometryInputFunSpec.NullableVarBinary<STGeomFromEWKB>() {

  init {
    testGeometryInput(
      "Calling ST_GeomFromEWKT on an EWKB POINT",
      byteArrayOf(1, 1, 0, 0, 32, -26, 16, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63),
      byteArrayOf(1, 1, 0, 0, 32, -26, 16, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63)
    )

    testInvalidGeometryInput(
      "Calling ST_GeomFromEWKB on rubbish data",
      byteArrayOf(12, 9, 8, 11, 2, 0, 9, 10, 0, 0,11,22, 33, 0, 0, 52, 53, 0, 22, 0, 0, 0, 0, -32, 63),
    )

    testGeometryInput(
      "Calling ST_GeomFromEWKB on WKB",
      byteArrayOf(1, 1, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63),
      byteArrayOf(1, 1, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63),
    )

    testNullGeometryInput(
      "Calling ST_GeomFromEWKB on null input"
    )
  }

  override val function = STGeomFromEWKB().apply {
    ewkbInput = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STGeomFromEWKB.input: NullableVarBinaryHolder get() = function.ewkbInput
  override val STGeomFromEWKB.output: NullableVarBinaryHolder get() = function.binaryOutput
}
