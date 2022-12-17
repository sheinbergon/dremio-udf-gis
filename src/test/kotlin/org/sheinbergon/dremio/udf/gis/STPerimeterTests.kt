package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryMeasurementFunSpec

internal class STPerimeterTests : GeometryMeasurementFunSpec.Unary<STPerimeter>() {

  init {
    testGeometryMeasurement(
      "Calling ST_Perimeter on a POINT geometry returns 0",
      "POINT(1.92 345.214)",
      4326,
      0.0
    )

    testGeometryMeasurement(
      "Calling ST_Perimeter on a LINESTRING geometry returns 0",
      "LINESTRING(2.0 0.0,0.0 1.0)",
      4326,
      0.0
    )

    testGeometryMeasurement(
      "Calling ST_Perimeter on a POLYGON geometry stated in srid 2279 returns its perimeter calculation in ft",
      "POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))",
      2279,
      122.63074400009504
    )
  }

  override val function = STPerimeter().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }
  override val STPerimeter.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput
  override val STPerimeter.measurementOutput: NullableFloat8Holder get() = function.output
}
