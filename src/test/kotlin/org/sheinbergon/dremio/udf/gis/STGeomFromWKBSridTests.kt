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
      byteArrayOf(0, 32, 0, 0, 1, 0, 0, 125, 106, 63, -32, 0, 0, 0, 0, 0, 0, 63, -32, 0, 0, 0, 0, 0, 0)
    ) { function.sridInput.value = 32106 }
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
