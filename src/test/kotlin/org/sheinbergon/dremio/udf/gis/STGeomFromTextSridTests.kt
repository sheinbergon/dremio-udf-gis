package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryInputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STGeomFromTextSridTests : GeometryInputFunSpec.NullableVarChar<STGeomFromTextSrid>() {

  init {

    beforeEach {
      function.sridInput.reset()
    }

    testGeometryInput(
      "Calling ST_GeomFromText on a POINT",
      "POINT(0.5 0.5)",
      byteArrayOf(1, 1, 0, 0, 32, -25, 8, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63)
    ) { function.sridInput.value = 2279 }

    testNullGeometryInput(
      "Calling ST_GeomFromText (with SRID) on null input"
    ) { function.sridInput.value = 4326 }
  }

  override val function = STGeomFromTextSrid().apply {
    wktInput = NullableVarCharHolder()
    sridInput = IntHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STGeomFromTextSrid.input: NullableVarCharHolder get() = function.wktInput
  override val STGeomFromTextSrid.output: NullableVarBinaryHolder get() = function.binaryOutput
}
