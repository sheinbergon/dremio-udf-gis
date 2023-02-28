package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer

internal class STIsValidReasonFlagsTests : GeometryAccessorFunSpec<STIsValidReasonFlags, NullableVarCharHolder>() {

  init {
    testGeometryAccessor(
      "Calling ST_IsValidReason on a self-intersecting Polygon without the 'ESRI flag' set",
      "POLYGON((3 0, 0 3, 6 3, 3 0, 4 2, 2 2, 3 0))",
      "Ring Self-intersection [3.0 0.0]"
    ) { function.flagsInput.value = 0 }

    testGeometryAccessor(
      "Calling ST_IsValidReason on a self-intersecting Polygon with the 'ESRI flag' set",
      "POLYGON((3 0, 0 3, 6 3, 3 0, 4 2, 2 2, 3 0))",
      "Valid Geometry"
    ) { function.flagsInput.value = 1 }

    testNullGeometryAccessor(
      "Calling ST_IsValidReason on null input returns false",
    ) { function.flagsInput.value = 0 }
  }

  override val function = STIsValidReasonFlags().apply {
    binaryInput = NullableVarBinaryHolder()
    flagsInput = IntHolder()
    textOutput = NullableVarCharHolder()
    buffer = allocateBuffer()
  }

  override val STIsValidReasonFlags.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STIsValidReasonFlags.output: NullableVarCharHolder get() = function.textOutput
}
