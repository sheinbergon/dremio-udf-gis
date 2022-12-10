package org.sheinbergon.dremio.udf.gis.util

import com.dremio.sabot.exec.context.BufferManagerImpl
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableIntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.apache.arrow.vector.holders.VarCharHolder
import org.locationtech.jts.geom.PrecisionModel
import org.locationtech.jts.io.WKBWriter
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.precision.GeometryPrecisionReducer
import java.io.StringReader
import java.nio.charset.StandardCharsets

private val SCALED_PRECISION_MODEL = PrecisionModel(100000.0)

private val allocator = RootAllocator()

private val wktReader = WKTReader()

private val wkbWriter = WKBWriter(2, true)

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

internal fun NullableVarCharHolder.setUtf8(text: String) {
  val bytes = text.toByteArray(StandardCharsets.UTF_8)
  val buffer = allocator.buffer(bytes.size.toLong())
  buffer.writeBytes(bytes)
  this.isSet = 1
  this.start = 0
  this.end = bytes.size
  this.buffer = buffer
}

internal fun NullableVarBinaryHolder.setFromWkt(wkt: String, srid: Int? = null) {
  val geometry = wktReader.read(StringReader(wkt))
  srid?.let(geometry::setSRID)
  val bytes = wkbWriter.write(geometry)
  val buffer = allocator.buffer(bytes.size.toLong())
  buffer.writeBytes(bytes)
  this.isSet = 1
  this.start = 0
  this.end = buffer.capacity().toInt()
  this.buffer = buffer
}

internal fun NullableVarBinaryHolder.setBinary(bytes: ByteArray) {
  val buffer = allocator.buffer(bytes.size.toLong())
  buffer.writeBytes(bytes)
  this.isSet = 1
  this.start = 0
  this.end = buffer.capacity().toInt()
  this.buffer = buffer
}

internal fun NullableVarBinaryHolder.valueIsAsDescribedIn(text: String) {
  val evaluated = GeometryHelpers.toGeometry(this)
  val reduced = GeometryPrecisionReducer.reducePointwise(evaluated, SCALED_PRECISION_MODEL)
  val expected = NullableVarCharHolder()
    .apply { setUtf8(text) }
    .let(GeometryHelpers::toGeometry)
  GeometryHelpers.toBinary(reduced) shouldBe GeometryHelpers.toBinary(expected)
}

internal fun NullableVarBinaryHolder.valueIsNotSet() {
  GeometryHelpers.isHolderSet(this) shouldBe false
}

internal fun NullableVarBinaryHolder.reset() {
  end = 0
  start = 0
  isSet = 0
  buffer?.clear()
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
  buffer?.close()
  buffer = null
}

internal fun NullableVarCharHolder.release() {
  buffer?.close()
  buffer = null
}

internal fun NullableFloat8Holder.reset() {
  isSet = 0
  value = 0.0
}

internal fun Float8Holder.reset() {
  value = 0.0
}

internal fun NullableIntHolder.reset() {
  isSet = 0
  value = 0
}

internal fun IntHolder.reset() {
  value = 0
}

internal fun BitHolder.reset() {
  value = 0
}

internal fun NullableBitHolder.reset() {
  isSet = 0
  value = 0
}

internal fun NullableBitHolder.valueIsTrue() = this.value shouldBeExactly 1
internal fun NullableBitHolder.valueIsFalse() = this.value shouldBeExactly 0
internal fun BitHolder.valueIsTrue() = this.value shouldBeExactly 1
internal fun BitHolder.valueIsFalse() = this.value shouldBeExactly 0
