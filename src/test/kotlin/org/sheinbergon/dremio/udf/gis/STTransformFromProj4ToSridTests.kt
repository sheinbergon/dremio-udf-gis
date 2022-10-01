package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryTransformationFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setUtf8

internal class STTransformFromProj4ToSridTests : GeometryTransformationFunSpec<STTransformFromProj4ToSrid>() {

  init {

    beforeEach {
      function.sourceProj4ParametersInput.reset()
      function.targetSridInput.reset()
    }

    afterEach {
      function.sourceProj4ParametersInput.release()
    }

    testGeometryTransformation(
      name = "Calling ST_TRANSFORM on a WGS84 POLYGON to transform it using a proj4 string",
      wkt = "LINESTRING(1 1,2 3,4 8)",
      expected = "LINESTRING (37049474.20769 22132952.94676, 36764309.60766 22975620.40908, 35905658.85601 24882339.1246)"
    ) {
      function.targetSridInput.value = 2279
      function.sourceProj4ParametersInput.setUtf8("+proj=longlat +datum=WGS84 +no_defs")
    }
  }

  override val function = STTransformFromProj4ToSrid().apply {
    binaryInput = NullableVarBinaryHolder()
    targetSridInput = IntHolder()
    sourceProj4ParametersInput = NullableVarCharHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STTransformFromProj4ToSrid.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STTransformFromProj4ToSrid.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
