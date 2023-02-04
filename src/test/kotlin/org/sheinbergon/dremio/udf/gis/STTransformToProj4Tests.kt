package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryTransformationFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setUtf8

internal class STTransformToProj4Tests : GeometryTransformationFunSpec<STTransformToProj4>() {

  init {

    beforeEach {
      function.targetProj4ParametersInput.reset()
    }

    afterEach {
      function.targetProj4ParametersInput.release()
    }

    testGeometryTransformationWKT(
      name = "Calling ST_TRANSFORM on a WGS84 POLYGON to transform it using a proj4 string",
      wkt = "POLYGON((170 50,170 72,-130 72,-130 50,170 50))",
      sourceSrid = 4326,
      expected = """
      POLYGON (
        (
          -2252039.48455 -1829529.61316, 
          -1000248.99307 477783.10136, 
          1000248.99307 477783.10136, 
          2252039.48455 -1829529.61316, 
          -2252039.48455 -1829529.61316
        )
      )
      """.trimIndent()
    ) { function.targetProj4ParametersInput.setUtf8("+proj=gnom +ellps=WGS84 +lat_0=70 +lon_0=-160 +no_defs") }
  }

  override val function = STTransformToProj4().apply {
    binaryInput = NullableVarBinaryHolder()
    targetProj4ParametersInput = NullableVarCharHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STTransformToProj4.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STTransformToProj4.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
