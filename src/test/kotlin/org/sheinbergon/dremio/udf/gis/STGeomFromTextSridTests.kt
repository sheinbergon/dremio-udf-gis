package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryInputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STGeomFromTextSridTests : GeometryInputFunSpec.NullableVarChar<STGeomFromTextSrid>() {

  init {
    testGeometryInput(
      "Calling ST_GeomFromText on a POINT",
      "POINT(0.5 0.5)",
      byteArrayOf(0, 32, 0, 0, 1, 0, 0, 8, -25, 63, -32, 0, 0, 0, 0, 0, 0, 63, -32, 0, 0, 0, 0, 0, 0)
    ) { function.sridInput.value = 2279 }
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
