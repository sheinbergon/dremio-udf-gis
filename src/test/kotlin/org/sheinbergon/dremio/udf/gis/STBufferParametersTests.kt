package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setUtf8

internal class STBufferParametersTests : GeometryProcessingFunSpec<STBufferParameters>() {

  init {

    beforeEach {
      function.radiusInput.reset()
      function.parametersInput.reset()
    }

    testGeometryProcessing(
      name = "Calling ST_Buffer on a POINT with 'quad_segs=2'",
      wkt = "POINT(100 90)",
      expected = """
        POLYGON (
          (
            150 90, 
            135.35534 54.64466, 
            100 40, 
            64.64466 54.64466, 
            50 90, 
            64.64466 125.35534, 
            100 140, 
            135.35534 125.35534, 
            150 90
          )
        )
      """.trimIndent()
    ) {
      function.radiusInput.value = 50.0
      function.parametersInput.setUtf8("quad_segs=2")
    }

    testGeometryProcessing(
      name = "Calling ST_Buffer on a LINESTRING with 'endcap=flat join=round'",
      wkt = "LINESTRING(50 50,150 150,150 50)",
      expected = """
        POLYGON (
          (
            142.92893 157.07107, 
            144.4443 158.3147, 
            146.17317 159.2388, 
            148.0491 159.80785, 
            150 160, 
            151.9509 159.80785, 
            153.82683 159.2388, 
            155.5557 158.3147, 
            157.07107 157.07107, 
            158.3147 155.5557, 
            159.2388 153.82683, 
            159.80785 151.9509, 
            160 150, 
            160 50, 
            140 50, 
            140 125.85786, 
            57.07107 42.92893, 
            42.92893 57.07107, 
            142.92893 157.07107
          )
        )
      """.trimIndent()
    ) {
      function.radiusInput.value = 10.0
      function.parametersInput.setUtf8("endcap=flat join=round")
    }

    testGeometryProcessing(
      name = "Calling ST_Buffer on a LINESTRING with 'join=mitre mitre_limit=5.0'",
      wkt = "LINESTRING(50 50,150 150,150 50)",
      expected = """
        POLYGON (
          (
            160 174.14214, 
            160 50, 
            159.80785 48.0491, 
            159.2388 46.17317, 
            158.3147 44.4443, 
            157.07107 42.92893, 
            155.5557 41.6853, 
            153.82683 40.7612, 
            151.9509 40.19215, 
            150 40, 
            148.0491 40.19215, 
            146.17317 40.7612, 
            144.4443 41.6853, 
            142.92893 42.92893, 
            141.6853 44.4443, 
            140.7612 46.17317, 
            140.19215 48.0491, 
            140 50, 
            140 125.85786, 
            57.07107 42.92893, 
            55.5557 41.6853, 
            53.82683 40.7612, 
            51.9509 40.19215, 
            50 40, 
            48.0491 40.19215, 
            46.17317 40.7612, 
            44.4443 41.6853, 
            42.92893 42.92893, 
            41.6853 44.4443, 
            40.7612 46.17317, 
            40.19215 48.0491, 
            40 50, 
            40.19215 51.9509, 
            40.7612 53.82683, 
            41.6853 55.5557, 
            42.92893 57.07107, 
            160 174.14214
          )
        )
      """.trimIndent()
    ) {
      function.radiusInput.value = 10.0
      function.parametersInput.setUtf8("join=mitre mitre_limit=5.0")
    }

    testGeometryProcessing(
      name = "Calling ST_Buffer on a POLYGON with 'side=right'",
      wkt = "POLYGON ((50 50, 50 150, 150 150, 150 50, 50 50))",
      expected = "POLYGON ((70 70, 70 130, 130 130, 130 70, 70 70))"
    ) {
      function.radiusInput.value = 20.0
      function.parametersInput.setUtf8("side=right")
    }

    testNullGeometryProcessing(
      "Calling ST_Buffer (with parameters string) on a NULL input"
    ) {
      function.radiusInput.value = 29.1
      function.parametersInput.setUtf8("side=right")
    }
  }

  override val function = STBufferParameters().apply {
    binaryInput = NullableVarBinaryHolder()
    radiusInput = Float8Holder()
    parametersInput = NullableVarCharHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STBufferParameters.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STBufferParameters.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
