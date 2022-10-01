package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryMeasurementFunSpec

internal class STAngle3PointsTests : GeometryMeasurementFunSpec.Ternary<STAngle3Points>() {

  init {
    testGeometryMeasurement(
      "Calling ST_Angle on 3 POINTs returns the angle in radians",
      "POINT(0 0)",
      "POINT(10 10)",
      "POINT(20 0)",
      4.71238898038469
    )
  }

  override val function = STAngle3Points().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    binaryInput3 = NullableVarBinaryHolder()
    output = Float8Holder()
  }

  override val STAngle3Points.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STAngle3Points.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STAngle3Points.wkbInput3: NullableVarBinaryHolder get() = function.binaryInput3
  override val STAngle3Points.measurementOutput: Float8Holder get() = function.output
}
