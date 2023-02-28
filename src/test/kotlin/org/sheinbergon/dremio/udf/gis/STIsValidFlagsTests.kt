package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STIsValidFlagsTests : GeometryAccessorFunSpec<STIsValidFlags, NullableBitHolder>() {

  init {

    beforeEach {
      function.flagsInput.reset()
    }

    testGeometryAccessor(
      "Calling ST_IsValid on a self-intersecting Polygon without the 'ESRI flag' set",
      "POLYGON((3 0, 0 3, 6 3, 3 0, 4 2, 2 2, 3 0))",
      false
    ) { function.flagsInput.value = 0 }

    testGeometryAccessor(
      "Calling ST_IsValid on a self-intersecting Polygon with the 'ESRI flag' set",
      "POLYGON((3 0, 0 3, 6 3, 3 0, 4 2, 2 2, 3 0))",
      true
    ) { function.flagsInput.value = 1 }

    testGeometryAccessor(
      "Calling ST_IsValid on a valid LineString without the 'ESRI flag' set",
      "LINESTRING(0 0, 1 1)",
      true
    ) { function.flagsInput.value = 1 }

    testNullGeometryAccessor(
      "Calling ST_IsValid on null input returns false without the 'ESRI flag' set",
    ) { function.flagsInput.value = 1 }
  }

  override val function = STIsValidFlags().apply {
    binaryInput = NullableVarBinaryHolder()
    flagsInput = IntHolder()
    output = NullableBitHolder()
  }

  override val STIsValidFlags.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STIsValidFlags.output: NullableBitHolder get() = function.output
}
