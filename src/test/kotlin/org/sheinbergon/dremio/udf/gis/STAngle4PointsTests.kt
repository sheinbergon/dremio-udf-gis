package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryMeasurementFunSpec

internal class STAngle4PointsTests : GeometryMeasurementFunSpec.Quaternary<STAngle4Points>() {

  init {
    testGeometryMeasurement(
      "Calling ST_Angle on 4 POINTs returns the angle in radians",
      "POINT(10 10)",
      "POINT(0 0)",
      "POINT(90 90)",
      "POINT(100 80)",
      4.71238898038469
    )

    testNullGeometryMeasurement(
      "Calling ST_Angle on 4 POINTs, one of them is null",
      "POINT(10 10)",
      null,
      "POINT(10 20)",
      "POINT(6 29)",
      4326
    )
  }

  override val function = STAngle4Points().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    binaryInput3 = NullableVarBinaryHolder()
    binaryInput4 = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }

  override val STAngle4Points.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STAngle4Points.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STAngle4Points.wkbInput3: NullableVarBinaryHolder get() = function.binaryInput3
  override val STAngle4Points.wkbInput4: NullableVarBinaryHolder get() = function.binaryInput4
  override val STAngle4Points.measurementOutput: NullableFloat8Holder get() = function.output
}
