package org.sheinbergon.dremio.udf.gis.util

import com.dremio.sabot.exec.context.BufferManagerImpl
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.apache.arrow.vector.holders.VarCharHolder
import org.locationtech.jts.io.WKBWriter
import org.locationtech.jts.io.WKTReader
import java.io.StringReader
import java.nio.charset.StandardCharsets

private val allocator = RootAllocator()

private val reader = WKTReader()

private val writer = WKBWriter(2, true)

private val manager = BufferManagerImpl(allocator)

internal fun allocateBuffer() = manager.managedBuffer

internal fun VarCharHolder.setUtf8(text: String) {
  val bytes = text.toByteArray(StandardCharsets.UTF_8)
  val buffer = allocator.buffer(bytes.size.toLong())
  buffer.writeBytes(bytes)
  this.start = 0
  this.end = bytes.size
  this.buffer = buffer
}

internal fun NullableVarBinaryHolder.setFromWkt(wkt: String, srid: Int? = null) {
  val geometry = reader.read(StringReader(wkt))
  srid?.let(geometry::setSRID)
  val bytes = writer.write(geometry)
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

internal fun VarCharHolder.reset() {
  end = 0
  start = 0
  buffer = null
}

internal fun NullableVarCharHolder.reset() {
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
