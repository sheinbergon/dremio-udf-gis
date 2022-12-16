package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryOutputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STAsGeoJsonTests : GeometryOutputFunSpec<STAsGeoJson>() {

  init {
    testGeometryOutput(
      "Calling ST_AsGeoJson on a POINT",
      "POINT(0.5 0.5)",
      4326,
      """
        {"type":"Point","coordinates":[0.5,0.5],"crs":{"type":"name","properties":{"name":"EPSG:4326"}}}
      """.trimIndent()
    )

    testGeometryOutput(
      "Calling ST_AsGeoJson on a POLYGON",
      "POLYGON((0.0 0.0,1.23 0.0,1.0 1.0,0.19 1.0,0.0 0.0))",
      3857,
      """
        {"type":"Polygon","coordinates":[[[0.0,0.0],[1.23,0.0],[1,1],[0.19,1],[0.0,0.0]]],"crs":{"type":"name","properties":{"name":"EPSG:3857"}}}
      """.trimIndent()
    )

    testNullGeometryOutput(
      "Calling ST_AsGeoJson on null input",
    )
  }

  override val function = STAsGeoJson().apply {
    binaryInput = NullableVarBinaryHolder()
    geoJsonOutput = NullableVarCharHolder()
    buffer = allocateBuffer()
  }

  override val STAsGeoJson.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STAsGeoJson.output: NullableVarCharHolder get() = function.geoJsonOutput
}
