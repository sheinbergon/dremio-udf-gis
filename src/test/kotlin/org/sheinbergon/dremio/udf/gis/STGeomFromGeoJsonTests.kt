package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryInputFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STGeomFromGeoJsonTests : GeometryInputFunSpec.NullableVarChar<STGeomFromGeoJson>() {

  init {
    testGeometryInput(
      "Calling ST_GeomFromGeoJSON on a POINT",
      """
        {"type":"Point","coordinates":[0.5,0.5]}
      """.trimIndent(),
      byteArrayOf(1, 1, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -32, 63, 0, 0, 0, 0, 0, 0, -32, 63)
    )

    testInvalidGeometryInput(
      "Calling ST_GeomFromGeoJSON on rubbish text",
      "42ifon2 fA!@",
    )

    testNullGeometryInput(
      "Calling ST_GeomFromGeoJSON on null input"
    )
  }

  override val function = STGeomFromGeoJson().apply {
    jsonInput = NullableVarCharHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STGeomFromGeoJson.input: NullableVarCharHolder get() = function.jsonInput
  override val STGeomFromGeoJson.output: NullableVarBinaryHolder get() = function.binaryOutput
}
