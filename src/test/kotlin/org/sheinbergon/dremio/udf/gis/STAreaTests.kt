package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryMeasurementFunSpec

internal class STAreaTests : GeometryMeasurementFunSpec.Unary<STArea>() {

  init {
    testGeometryMeasurement(
      "Calling ST_Area on a POINT geometry returns 0",
      "POINT(1.92 345.214)",
      4326,
      0.0
    )

    testGeometryMeasurement(
      "Calling ST_Area on a LINESTRING geometry returns 0",
      "LINESTRING(2.0 0.0,0.0 1.0)",
      4326,
      0.0
    )

    testGeometryMeasurement(
      "Calling ST_Area on a POLYGON geometry stated in srid 2279 returns its area calculation in sqft",
      "POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 2967416,743238 2967416))",
      2279,
      928.625
    )

    testNullGeometryMeasurement(
      "Calling ST_Area on a NULL input"
    )
  }

  override val function = STArea().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }
  override val STArea.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput
  override val STArea.measurementOutput: NullableFloat8Holder get() = function.output
}
