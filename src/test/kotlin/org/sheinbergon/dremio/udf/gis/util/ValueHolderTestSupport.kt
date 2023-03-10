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
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.PrecisionModel
import org.locationtech.jts.io.InputStreamInStream
import org.locationtech.jts.io.WKBReader
import org.locationtech.jts.io.WKBWriter
import org.locationtech.jts.io.WKTReader
import java.io.StringReader
import java.nio.charset.StandardCharsets

private val SCALED_PRECISION_MODEL = PrecisionModel(100000.0)

private val allocator = RootAllocator()

private val wktReader = WKTReader()

private val wkbWriter = WKBWriter(2, true)

private val manager = BufferManagerImpl(allocator)

internal fun allocateBuffer() = manager.managedBuffer

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

internal fun NullableVarBinaryHolder.valueIsAsDescribedInWKT(text: String) =
  valueIsAsDescribedIn(text, ::toGeometryFromWKT, GeometryHelpers::toBinary)

internal fun NullableVarBinaryHolder.valueIsAsDescribedInEWKT(text: String) =
  valueIsAsDescribedIn(text, ::toGeometryFromEWKT, GeometryHelpers::toEWKB)

internal fun NullableVarBinaryHolder.valueIsAsDescribedIn(
  text: String,
  adapter: (NullableVarCharHolder) -> Geometry,
  serializer: (Geometry) -> ByteArray,
) {
  val evaluated = toGeometry(this)
  val expected = NullableVarCharHolder()
    .apply { setUtf8(text) }
    .let(adapter)
  serializer(evaluated) shouldBe serializer(expected)
  this.isSet shouldBeExactly 1
}

private fun toGeometry(holder: NullableVarBinaryHolder) = holder.buffer?.run {
  val buffer = holder.buffer.nioBuffer(holder.start.toLong(), holder.end - holder.start)
  ByteBufferInputStream.toInputStream(buffer).use { stream ->
    val adapter = InputStreamInStream(stream)
    val reader = WKBReader(GeometryFactory(SCALED_PRECISION_MODEL))
    reader.read(adapter)
  }
} ?: GeometryHelpers.emptyGeometry()

private fun toGeometryFromWKT(holder: NullableVarCharHolder): Geometry {
  val wkt = GeometryHelpers.toUTF8String(holder)
  val reader = WKTReader(GeometryFactory(SCALED_PRECISION_MODEL))
  return reader.read(wkt)
}

private fun toGeometryFromEWKT(
  holder: NullableVarCharHolder
): Geometry {
  val ewkt = GeometryHelpers.toUTF8String(holder)
  val reader = WKTReader(GeometryFactory(SCALED_PRECISION_MODEL))
  val matcher = GeometryHelpers.EWKT_REGEX_PATTERN.matcher(ewkt)
  return if (matcher.find()) {
    reader
      .read(matcher.group(2))
      .also { geometry ->
        geometry.srid = matcher.group(1).toInt()
      }
  } else {
    throw IllegalArgumentException("input '$ewkt' is not a valid EWKT")
  }
}

internal fun NullableVarBinaryHolder.valueIsNotSet() {
  GeometryHelpers.isHolderSet(this) shouldBe false
}

internal fun NullableVarCharHolder.valueIsNotSet() =
  GeometryHelpers.isHolderSet(this) shouldBe false

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

internal fun NullableBitHolder.valueIsTrue() = (this.value shouldBeExactly 1) and (this.isSet shouldBeExactly 1)
internal fun NullableBitHolder.valueIsFalse() = (this.value shouldBeExactly 0) and (this.isSet shouldBeExactly 1)
