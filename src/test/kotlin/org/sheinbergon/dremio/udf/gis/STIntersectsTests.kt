package org.sheinbergon.dremio.udf.gis

import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.spec.GeometryRelationFunSpec

internal class STIntersectsTests : GeometryRelationFunSpec<STIntersects>() {

    init {
        testTrueGeometryRelation(
            "Calling ST_Intersects on a POINT within a POLYGON",
            "POINT(0.5 0.5)",
            "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
        )

        testFalseGeometryRelation(
            "Calling ST_Intersects on a POINT outside of a POLYGON",
            "POINT(22.5 0.5)",
            "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
        )

        testTrueGeometryRelation(
            "Calling ST_Intersects on a POINT touching a POLYGON",
            "POINT(0.0 0.5)",
            "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
        )

        testTrueGeometryRelation(
            "Calling ST_Intersects on a LINESTRING intersecting with a POLYGON",
            "LINESTRING(2.0 0.0,0.0 1.0)",
            "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
        )

        testTrueGeometryRelation(
            "Calling ST_Intersects on a LINESTRING crossing a POLYGON",
            "LINESTRING(2.0 0.5,-2.0 0.6)",
            "POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))"
        )
    }

    override val function = STIntersects().apply {
        binaryInput1 = NullableVarBinaryHolder()
        binaryInput2 = NullableVarBinaryHolder()
        output = BitHolder()
    }

    override val STIntersects.wkbInput1: NullableVarBinaryHolder get() = function.binaryInput1
    override val STIntersects.wkbInput2: NullableVarBinaryHolder get() = function.binaryInput2
    override val STIntersects.output: BitHolder get() = function.output
}