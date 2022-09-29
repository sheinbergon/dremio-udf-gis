package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometrySpatialReferenceSystemFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setUtf8

internal class STTransformToProj4Tests : GeometrySpatialReferenceSystemFunSpec<STTransformToProj4>() {

  init {

    beforeEach {
      function.targetProj4ParametersInput.reset()
    }

    afterEach {
      function.targetProj4ParametersInput.release()
    }

    testGeometryTransformation(
      "Calling ST_TRANSFORM on a WGS84 POLYGON to transform it using a proj4 string",
      "POLYGON((170 50,170 72,-130 72,-130 50,170 50))",
      4326,
      """
      POLYGON (
        (
          -2252039.4845483867 -1829529.613162698,
          -1000248.9930653168 477783.1013576727, 
          1000248.9930653175 477783.10135767306, 
          2252039.484548388 -1829529.6131626973, 
          -2252039.4845483867 -1829529.613162698
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
