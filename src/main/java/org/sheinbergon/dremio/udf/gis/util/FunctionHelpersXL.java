/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.sheinbergon.dremio.udf.gis.util;

import org.apache.arrow.vector.holders.NullableFloat8Holder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class FunctionHelpersXL {

  static final int BIT_TRUE = 1;
  static final int BIT_FALSE = 0;
  static final String POINT = "Point";
  public static final int DEFAULT_SRID = 4326;

  private static final int GEOMETRY_WRITER_DIMENSIONS = 2;

  public static java.lang.String toUTF8String(
      final @Nonnull org.apache.arrow.vector.holders.VarCharHolder holder) {
    return com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
        holder.start,
        holder.end,
        holder.buffer);
  }

  private static java.lang.String toUTF8String(
      final @Nonnull org.apache.arrow.vector.holders.NullableVarCharHolder holder) {
    return com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
        holder.start,
        holder.end,
        holder.buffer);
  }

  public static byte[] toBinary(
      final @Nonnull org.locationtech.jts.geom.Geometry geometry) {
    org.locationtech.jts.io.WKBWriter writer = new org.locationtech.jts.io.WKBWriter(GEOMETRY_WRITER_DIMENSIONS, true);
    return writer.write(geometry);
  }

  public static byte[] toText(
      final @Nonnull org.locationtech.jts.geom.Geometry geometry) {
    org.locationtech.jts.io.WKTWriter writer = new org.locationtech.jts.io.WKTWriter(GEOMETRY_WRITER_DIMENSIONS);
    return writer.write(geometry).getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  public static byte[] toGeoJson(
      final @Nonnull org.locationtech.jts.geom.Geometry geometry) {
    org.locationtech.jts.io.geojson.GeoJsonWriter writer = new org.locationtech.jts.io.geojson.GeoJsonWriter();
    return writer.write(geometry).getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  @Nonnull
  public static org.locationtech.jts.geom.Geometry toGeometry(
      final @Nonnull org.apache.arrow.vector.holders.NullableVarCharHolder holder
  ) {
    try {
      java.lang.String wkt = toUTF8String(holder);
      org.locationtech.jts.io.WKTReader reader = new org.locationtech.jts.io.WKTReader();
      return reader.read(wkt);
    } catch (org.locationtech.jts.io.ParseException x) {
      throw new RuntimeException(x);
    }
  }

  public static org.locationtech.jts.geom.Geometry toGeometry(
      final @Nonnull org.apache.arrow.vector.holders.NullableVarBinaryHolder holder
  ) {
    java.nio.ByteBuffer buffer = holder.buffer.nioBuffer(holder.start, holder.end - holder.start);
    try (java.io.InputStream stream = org.sheinbergon.dremio.udf.gis.util.ByteBufferInputStream.toInputStream(buffer)) {
      org.locationtech.jts.io.InputStreamInStream adapter = new org.locationtech.jts.io.InputStreamInStream(stream);
      org.locationtech.jts.io.WKBReader reader = new org.locationtech.jts.io.WKBReader();
      return reader.read(adapter);
    } catch (java.io.IOException | org.locationtech.jts.io.ParseException x) {
      throw new RuntimeException(x);
    }
  }

  public static int toBitValue(final boolean value) {
    return value ? BIT_TRUE : BIT_FALSE;
  }

  private FunctionHelpersXL() {
  }

  public static void populate(
      final @Nonnull byte[] bytes,
      final @Nonnull org.apache.arrow.memory.ArrowBuf buffer,
      final @Nonnull org.apache.arrow.vector.holders.NullableVarCharHolder output) {
    output.buffer = buffer;
    output.start = 0;
    output.end = bytes.length;
    output.buffer.setBytes(output.start, bytes);
  }

  public static void populate(
      final @Nonnull byte[] bytes,
      final @Nonnull org.apache.arrow.memory.ArrowBuf buffer,
      final @Nonnull org.apache.arrow.vector.holders.NullableVarBinaryHolder output) {
    output.buffer = buffer;
    output.start = 0;
    output.end = bytes.length;
    output.buffer.setBytes(output.start, bytes);
  }

  public static double envelope(
      final @Nullable org.locationtech.jts.geom.Geometry geometry,
      final @Nonnull java.util.function.Function<org.locationtech.jts.geom.Envelope, Double> getter) {
    return getter.apply(geometry.getEnvelopeInternal());
  }

  public static boolean isAPoint(
      final @Nullable org.locationtech.jts.geom.Geometry geometry) {
    return geometry != null && geometry.getGeometryType().equals(POINT);
  }

  public static boolean isHolderSet(final @Nonnull org.apache.arrow.vector.holders.ValueHolder holder) {
    if (holder instanceof org.apache.arrow.vector.holders.NullableIntHolder) {
      return ((org.apache.arrow.vector.holders.NullableIntHolder) holder).isSet == BIT_TRUE;
    } else {
      throw new java.lang.IllegalArgumentException(
          java.lang.String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  public static void extractY(
      @Nullable final org.locationtech.jts.geom.Geometry geometry,
      @Nonnull final NullableFloat8Holder output) {
    if (isAPoint(geometry)) {
      output.value = ((org.locationtech.jts.geom.Point) geometry).getY();
      output.isSet = BIT_TRUE;
    } else {
      output.isSet = BIT_FALSE;
    }
  }

  public static void extractX(
      @Nullable final org.locationtech.jts.geom.Geometry geometry,
      @Nonnull final NullableFloat8Holder output) {
    if (isAPoint(geometry)) {
      output.value = ((org.locationtech.jts.geom.Point) geometry).getX();
      output.isSet = BIT_TRUE;
    } else {
      output.isSet = BIT_FALSE;
    }
  }
}
