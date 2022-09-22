package org.sheinbergon.dremio.udf.gis.util

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.locationtech.jts.io.WKBWriter
import org.locationtech.jts.io.WKTReader
import java.io.StringReader

private val allocator = RootAllocator()

private val reader = WKTReader()

private val writer = WKBWriter()

internal fun NullableVarBinaryHolder.setFromWkt(wkt: String) {
    val bytes = writer.write(reader.read(StringReader(wkt)))
    val buffer = allocator.buffer(bytes.size.toLong())
    buffer.writeBytes(bytes)
    this.isSet = 1
    this.start = 0
    this.end = buffer.capacity().toInt()
    this.buffer = buffer
}

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

internal fun Float8Holder.reset() {
    value = 0.0
}

internal fun BitHolder.reset() {
    value = 0
}