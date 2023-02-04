package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOutputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STAsBinaryTests : GeometryOutputFunSpec.NullableVarBinary<STAsBinary>() {

  init {
    testGeometryWKTOutput(
      "Calling ST_AsBinary on a POINT",
      "POINT(0.5 0.5)",
      "POINT (0.5 0.5)"
    )

    testGeometryWKTOutput(
      "Calling ST_AsBinary on a POLYGON",
      "POLYGON((0.0 0.0,1.23 0.0,1.0 1.0,0.19 1.0,0.0 0.0))",
      "POLYGON ((0 0, 1.23 0, 1 1, 0.19 1, 0 0))"
    )

    testNullGeometryOutput(
      "Calling ST_AsBinary on null input",
    )
  }

  override val function = STAsBinary().apply {
    binaryInput = NullableVarBinaryHolder()
    wkbOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STAsBinary.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STAsBinary.output: NullableVarBinaryHolder get() = function.wkbOutput
}
