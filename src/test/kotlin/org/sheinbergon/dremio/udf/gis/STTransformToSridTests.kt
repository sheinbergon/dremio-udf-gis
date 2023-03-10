package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryTransformationFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STTransformToSridTests : GeometryTransformationFunSpec<STTransformToSrid>() {

  init {

    beforeEach {
      function.targetSridInput.reset()
    }

    testGeometryTransformationEWKT(
      name = "Calling ST_TRANSFORM on a WebMercator point to transform it to WGS84",
      wkt = "POINT (7910240.56433 5215074.23966)",
      sourceSrid = 3857,
      expected = "SRID=4326;POINT (71.0589 42.3601)"
    ) { function.targetSridInput.value = 4326 }

    testGeometryTransformationEWKT(
      name = "Calling ST_TRANSFORM on a doughnut MULTIPOLYGON",
      wkt = """
        MULTIPOLYGON((
          (
            3301892.7081 2305424.6332, 
            3297641.4146 2293854.3149, 
            3311252.1571 2288743.8019,  
            3292652.3093 2299850.1394, 
            3294313.4361 2305713.7012, 
            3301892.7081 2305424.6332
          ), 
          (
            3270969.4748 2271758.2972,
            3272874.4151 2267708.8757,
            3275976.4969 2266056.1655,
            3278803.0097 2276290.0461, 
            3273515.1866 2277201.8981, 
            3270969.4748 2271758.2972
          )
        ))
      """.replace('\n', ' '),
      sourceSrid = 3035,
      expected = """
        SRID=4326;MULTIPOLYGON((
          (
            -2.546387000278512 43.08800899493393,
            -2.575743178434398 42.97881974838686,
            -2.401407821787952 42.953749749806,
            -2.647614448413054 43.02455267884065,
            -2.638786226764408 43.07914976493757,
            -2.546387000278512 43.08800899493393
          ),
          (
            -2.85513603155533 42.74197265119143,
            -2.824354352347231 42.70893582158278,
            -2.783822206487717 42.69900705701716,
            -2.769511302427943 42.79422494822382,
            -2.83502342149246 42.79422494807209,
            -2.85513603155533 42.74197265119143
            )
          )
        ))
      """.replace('\n', ' ')
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
