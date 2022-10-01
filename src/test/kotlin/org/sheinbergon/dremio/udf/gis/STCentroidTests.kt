package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STCentroidTests : GeometryProcessingFunSpec<STCentroid>() {

  init {
    testGeometryProcessing(
      "Calling ST_Centroid on a MULTIPOINT collection",
      "MULTIPOINT(-1 0,-1 2,-1 3,-1 4,-1 7,0 1,0 3,1 1,2 0,6 0,7 8,9 8,10 6)",
      "POINT (2.30769 3.30769)"
    )
  }

  override val function = STCentroid().apply {
    binaryInput = NullableVarBinaryHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STCentroid.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STCentroid.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
