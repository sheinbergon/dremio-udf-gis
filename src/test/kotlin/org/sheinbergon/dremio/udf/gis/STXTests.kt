package org.sheinbergon.dremio.udf.gis

import io.kotest.core.spec.style.FunSpec
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.locationtech.jts.io.WKBWriter
import org.locationtech.jts.io.WKTReader
import java.io.StringReader


private val allocator = RootAllocator()

private val reader = WKTReader()

private val writer = WKBWriter()

internal fun NullableVarBinaryHolder.reset() {
    end = 0
    start = 0
    isSet = 0
    buffer = null
}

internal fun NullableVarBinaryHolder.release() {
    buffer.close()
}

internal fun NullableFloat8Holder.reset() {
    isSet = 0
    value = 0.0
}

internal fun NullableVarBinaryHolder.setGeometery(wkt: String) {
    val bytes = writer.write(reader.read(StringReader(wkt)))
    val buffer = allocator.buffer(bytes.size.toLong())
    buffer.writeBytes(bytes)
    this.isSet = 1
    this.start = 0
    this.end = buffer.capacity().toInt()
    this.buffer = buffer
}

internal fun NullableFloat8Holder.isSetTo(double: Double) = run {
    isSet == 1 && value == double
}

internal fun NullableFloat8Holder.isNotSet() {
    isSet == 0 && value == 0.0
}

internal class STXTests : FunSpec({
    val function = STX()
    function.binaryInput = NullableVarBinaryHolder()
    function.output = NullableFloat8Holder()

    beforeEach {
        function.binaryInput.reset()
        function.output.reset()
    }

    afterEach {
        function.binaryInput.release()
    }

    test("Passing a POINT geometry as input to ST_X should return the POINT's X value") {
        function.binaryInput.setGeometery("POINT(5.2 120.5)")
        function.setup()
        function.eval()
        function.output.isSetTo(5.2)
    }

    test("Passing a LINESTRING geometry as input to ST_X should return 0") {
        function.binaryInput.setGeometery("LINESTRING(5.2 120.5,7.3 122.5,7.9 130.9)")
        function.setup()
        function.eval()
        function.output.isNotSet()
    }
})