package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryAccessorFunSpec

internal class STIsClosedTests : GeometryAccessorFunSpec<STIsClosed, NullableBitHolder>() {

  init {
    testGeometryAccessor(
      "Calling ST_IsClosed on a POINT geometry returns false",
      "POINT(1.92 345.214)",
      true
    )

    testGeometryAccessor(
      "Calling ST_IsClosed on a closed LINESTRING  returns true",
      "LINESTRING(0 0, 0 1, 1 1, 0 0)",
      true
    )

    testGeometryAccessor(
      "Calling ST_IsClosed on a valid POLYGON returns true",
      "POLYGON((0 0, 0 1, 1 1, 0 0))",
      true
    )

    testThrowingGeometryAccessor(
      "Calling ST_IsClosed on an invalid LINEARRING returns true",
      "POLYGON((0 0, 0 1, 1 1, 0.5 0.5))",
      "Points of LinearRing do not form a closed linestring"
    )

    testThrowingGeometryAccessor(
      "Calling ST_IsClosed on an invalid POLYGON throws an exception",
      "POLYGON((0 0, 0 1, 1 1, 0.5 0.5))",
      "Points of LinearRing do not form a closed linestring"
    )

    testNullGeometryAccessor(
      "Calling ST_IsClosed on null input returns false",
    )
  }

  override val function = STIsClosed().apply {
    binaryInput = NullableVarBinaryHolder()
    output = NullableBitHolder()
  }

  override val STIsClosed.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STIsClosed.output: NullableBitHolder get() = function.output
}
