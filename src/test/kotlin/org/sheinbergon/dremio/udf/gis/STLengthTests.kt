package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryMeasurementFunSpec

internal class STLengthTests : GeometryMeasurementFunSpec.Unary<STLength>() {

  init {
    testGeometryMeasurement(
      "Calling ST_Length on a POINT geometry returns 0",
      "POINT(1.92 345.214)",
      4326,
      0.0
    )

    testGeometryMeasurement(
      "Calling ST_Length on a POLYGON geometry returns 0",
      "POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))",
      2279,
      0.0
    )

    testGeometryMeasurement(
      "Calling ST_Length on a LINESTRING geometry stated in srid 32608 returns it's length in meters",
      "LINESTRING(576100 15230, 576102 15230)",
      32608,
      2.0
    )

    testNullGeometryMeasurement(
      "Calling ST_Length on a NULL input"
    )
  }

  override val function = STLength().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }
  override val STLength.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput
  override val STLength.measurementOutput: NullableFloat8Holder get() = function.output
}
