package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryMeasurementFunSpec

internal class STDistanceTests : GeometryMeasurementFunSpec.Binary<STDistance>() {

  init {
    testGeometryMeasurement(
      "Calling ST_Angle on a POINT and a LINESTRING stated in srid 4326 returns their distance in degrees",
      "POINT(-72.1235 42.3521)",
      "LINESTRING(-72.1260 42.45, -72.123 42.1546)",
      4326,
      0.0015056772638228177
    )
  }

  override val function = STDistance().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }

  override val STDistance.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STDistance.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STDistance.measurementOutput: NullableFloat8Holder get() = function.output
}
