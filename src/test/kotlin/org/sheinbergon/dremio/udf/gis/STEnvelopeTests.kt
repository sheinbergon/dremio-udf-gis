package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STEnvelopeTests : GeometryProcessingFunSpec<STEnvelope>() {

  init {
    testGeometryProcessing(
      "Calling STEnvelope on a LINESTRING",
      "LINESTRING(0 0, 1 3)",
      "POLYGON((0 0,0 3,1 3,1 0,0 0))"
    )

    testNullGeometryProcessing(
      "Calling ST_Envelope on a NULL input"
    )
  }

  override val function = STEnvelope().apply {
    binaryInput = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STEnvelope.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STEnvelope.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
