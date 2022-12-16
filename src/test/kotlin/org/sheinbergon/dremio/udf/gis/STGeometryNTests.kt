package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryProcessingFunSpec
import org.sheinbergon.dremio.udf.gis.util.allocateBuffer
import org.sheinbergon.dremio.udf.gis.util.reset

internal class STGeometryNTests : GeometryProcessingFunSpec<STGeometryN>() {

  init {

    beforeEach {
      function.indexInput.reset()
    }

    testGeometryProcessing(
      name = "Calling ST_GeometryN on a GEOMETRYCOLLECTION to extract the second geometry in the collection",
      wkt = "GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
      expected = "LINESTRING (10 10, 20 20, 10 40)"
    ) { function.indexInput.value = 2 }

    testInvalidGeometryProcessing(
      name = "Calling ST_GeometryN on a LINESTRING with index different than 1",
      wkt = "LINESTRING (10 10, 20 20, 10 40)",
    ) { function.indexInput.value = 2 }

    testInvalidGeometryProcessing(
      name = "Calling ST_GeometryN on a GEOMETRYCOLLECTION  with an out-of-bound index",
      wkt = "GEOMETRYCOLLECTION (POINT (40 10),LINESTRING (10 10, 20 20, 10 40),POLYGON ((40 40, 20 45, 45 30, 40 40)))",
    ) { function.indexInput.value = 6 }

    testGeometryProcessing(
      name = "Calling ST_GeometryN on a POINT with index 1",
      wkt = "POINT (40 10)",
      expected = "POINT (40 10)"
    ) { function.indexInput.value = 1 }

    testInvalidGeometryProcessing(
      name = "Calling ST_GeometryN on an empty GEOMETRYCOLLECTION  with an out-of-bound index",
      wkt = "GEOMETRYCOLLECTION EMPTY",
    ) { function.indexInput.value = 0 }
  }

  override val function = STGeometryN().apply {
    binaryInput = NullableVarBinaryHolder()
    indexInput = IntHolder()
    binaryOutput = NullableVarBinaryHolder()
    buffer = allocateBuffer()
  }

  override val STGeometryN.wkbInput: NullableVarBinaryHolder get() = function.binaryInput
  override val STGeometryN.wkbOutput: NullableVarBinaryHolder get() = function.binaryOutput
}
