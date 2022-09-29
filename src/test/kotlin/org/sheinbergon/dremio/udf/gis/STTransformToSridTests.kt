package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometrySpatialReferenceSystemFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STTransformToSridTests : GeometrySpatialReferenceSystemFunSpec<STTransformToSrid>() {

  init {

    beforeEach {
      function.targetSridInput.reset()
    }

    testGeometryTransformation(
      "Calling ST_TRANSFORM on a WebMercator point to transform it to WGS84",
      "POINT (7910240.56433 5215074.23966)",
      3857,
      "POINT (71.05889999999869 42.36009999997683)"
    ) { function.targetSridInput.value = 4326 }
  }

  override val function = STTransformToSrid().apply {
    binaryInput = NullableVarBinaryHolder()
    targetSridInput = IntHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STTransformToSrid.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STTransformToSrid.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
