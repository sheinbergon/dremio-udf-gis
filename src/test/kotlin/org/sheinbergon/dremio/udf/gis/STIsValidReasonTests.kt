package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STIsValidReasonTests : GeometryAccessorFunSpec<STIsValidReason, NullableVarCharHolder>() {

  init {
    testGeometryAccessor(
      "Calling ST_IsValidReason on a self-intersecting Polygon",
      "POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))",
      "Ring Self-intersection [1.0 1.0]"
    )

    testGeometryAccessor(
      "Calling ST_IsValidReason on a valid LineString",
      "LINESTRING(0 0, 1 1)",
      "Valid Geometry"
    )

    testNullGeometryAccessor(
      "Calling ST_IsValidReason on null input returns false",
    )
  }

  override val function = STIsValidReason().apply {
    binaryInput = NullableVarBinaryHolder()
    textOutput = NullableVarCharHolder()
    buffer = allocateBuffer()
  }

  override val STIsValidReason.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STIsValidReason.output: NullableVarCharHolder get() = function.textOutput
}
