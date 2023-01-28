package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryMeasurementFunSpec

internal class STAzimuthTests : GeometryMeasurementFunSpec.Binary<STAzimuth>() {

  init {
    testGeometryMeasurement(
      "Calling ST_Azimuth on 2 POINTs returns the azimuth in radians",
      "POINT(25 45)",
      "POINT(75 100)",
      0.7378150601204648
    )

    testNullGeometryMeasurement(
      "Calling ST_Azimuth on a NULL input"
    )
  }

  override val function = STAzimuth().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }

  override val STAzimuth.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STAzimuth.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STAzimuth.measurementOutput: NullableFloat8Holder get() = function.output
}
