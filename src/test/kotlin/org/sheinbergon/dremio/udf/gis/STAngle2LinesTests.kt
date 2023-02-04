package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryMeasurementFunSpec

internal class STAngle2LinesTests : GeometryMeasurementFunSpec.Binary<STAngle2Lines>() {

  init {
    testGeometryMeasurement(
      "Calling ST_Angle on 2 LINESTRINGs returns the angle in radians",
      "LINESTRING(0 0, 0.3 0.7, 1 1)",
      "LINESTRING(0 0, 0.2 0.5, 1 0)",
      0.7853981633974483
    )

    testNullGeometryMeasurement(
      "Calling ST_Angle on 2 LINESTRINGs, one of them is null",
      "LINESTRING(0 0, 0.3 0.7, 1 1)",
      null,
      4326
    )
  }

  override val function = STAngle2Lines().apply {
    binaryInput1 = NullableVarBinaryHolder()
    binaryInput2 = NullableVarBinaryHolder()
    output = NullableFloat8Holder()
  }

  override val STAngle2Lines.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
  override val STAngle2Lines.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
  override val STAngle2Lines.measurementOutput: NullableFloat8Holder get() = function.output
}
