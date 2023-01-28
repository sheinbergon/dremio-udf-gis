package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryInputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STGeomFromWKBSridTests : GeometryInputFunSpec.NullableVarBinary<STGeomFromWKBSrid>() {

  init {

    beforeEach {
      function.sridInput.reset()
    }

    testGeometryInput(
      "Calling ST_GeomFromWKB on a POINT WKB representation",
      byteArrayOf(0, 32, 0, 0, 1, 0, 0, 0, 0, 63, -32, 0, 0, 0, 0, 0, 0, 63, -32, 0, 0, 0, 0, 0, 0),
      byteArrayOf(1, 1, 0, 0, 32, 106, 125, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63)
    ) { function.sridInput.value = 32106 }

    testNullGeometryInput(
      "Calling ST_GeomFromWKB (with SRID) on null input"
    ) { function.sridInput.value = 4326 }
  }

  override val function = STGeomFromWKBSrid().apply {
    wkbInput = NullableVarBinaryHolder()
    sridInput = IntHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STGeomFromWKBSrid.input: NullableVarBinaryHolder get() = function.wkbInput
  override val STGeomFromWKBSrid.output: NullableVarBinaryHolder get() = function.binaryOutput
}
