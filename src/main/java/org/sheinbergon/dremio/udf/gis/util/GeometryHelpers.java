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


import com.dremio.exec.expr.fn.impl.StringFunctionHelpers;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.*;
import org.apache.commons.io.IOUtils;
import org.locationtech.jts.algorithm.Angle;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.*;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.locationtech.jts.operation.buffer.BufferOp;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class GeometryHelpers {

  public static final int BIT_TRUE = 1;
  public static final int BIT_FALSE = 0;
  public static final int DEFAULT_SRID = 4326;

  private static final Set<String> AREAL_TYPES = Sets.newHashSet(Geometry.TYPENAME_POLYGON, Geometry.TYPENAME_MULTIPOLYGON);
  private static final Set<String> LINEAR_TYPES = Sets.newHashSet(Geometry.TYPENAME_LINESTRING, Geometry.TYPENAME_MULTILINESTRING);
  private static final int GEOMETRY_DIMENSIONS = 2;
  private static final double AZIMUTH_NORTH_RADIANS = Angle.toRadians(90.0);

  private static final Pattern EWKT_REGEX_PATTERN = Pattern.compile("^\\s*SRID\\s*=\\s*(\\d+)\\s*;\\s*(.+)\\s*$");

  private GeometryHelpers() {
  }

  public static Geometry emptyGeometry() {
    GeometryFactory factory = new GeometryFactory();
    return factory.createEmpty(GEOMETRY_DIMENSIONS);
  }

  public static String toUTF8String(final @Nonnull VarCharHolder holder) {
    return StringFunctionHelpers.toStringFromUTF8(
        holder.start,
        holder.end,
        holder.buffer);
  }

  public static String toUTF8String(final @Nonnull NullableVarCharHolder holder) {
    return StringFunctionHelpers.toStringFromUTF8(
        holder.start,
        holder.end,
        holder.buffer);
  }

  public static byte[] toEWKB(final @Nonnull Geometry geometry) {
    int order = nativeWkbByteOrder();
    WKBWriter writer = new WKBWriter(GEOMETRY_DIMENSIONS, order, true);
    return writer.write(geometry);
  }

  public static byte[] toBinary(final @Nonnull Geometry geometry) {
    int order = nativeWkbByteOrder();
    WKBWriter writer = new WKBWriter(GEOMETRY_DIMENSIONS, order, false);
    return writer.write(geometry);
  }

  private static int nativeWkbByteOrder() {
    return ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)
        ? ByteOrderValues.BIG_ENDIAN
        : ByteOrderValues.LITTLE_ENDIAN;
  }

  public static byte[] toText(
      final @Nonnull Geometry geometry) {
    WKTWriter writer = new WKTWriter(GEOMETRY_DIMENSIONS);
    return writer.write(geometry).getBytes(StandardCharsets.UTF_8);
  }

  public static byte[] toEWKT(
      final @Nonnull Geometry geometry) {
    final WKTWriter writer = new WKTWriter(GEOMETRY_DIMENSIONS);
    final String wkt = writer.write(geometry);
    String result;
    if (geometry.getSRID() > 0) {
      result = String.format("SRID=%d;%s", geometry.getSRID(), wkt);
    } else {
      result = wkt;
    }
    return result.getBytes(StandardCharsets.UTF_8);
  }

  public static byte[] toGeoJson(final @Nonnull Geometry geometry) {
    GeoJsonWriter writer = new GeoJsonWriter();
    return writer.write(geometry).getBytes(StandardCharsets.UTF_8);
  }

  @Nonnull
  public static Geometry toGeometry(final @Nonnull NullableVarCharHolder holder) {
    try {
      String wkt = toUTF8String(holder);
      WKTReader reader = new WKTReader();
      return reader.read(wkt);
    } catch (ParseException x) {
      throw new RuntimeException(x);
    }
  }

  @Nonnull
  public static Geometry toGeometryFromEWKT(final @Nonnull NullableVarCharHolder holder) {
    try {
      String ewkt = toUTF8String(holder);
      WKTReader reader = new WKTReader();
      Matcher matcher = EWKT_REGEX_PATTERN.matcher(ewkt);
      boolean found = matcher.find();
      if (found) {
        Geometry geometry = reader.read(matcher.group(2));
        geometry.setSRID(Integer.parseInt(matcher.group(1)));
        return geometry;
      } else {
        throw new IllegalArgumentException(
            String.format("input '%s' is not a valid EWKT",
                ewkt));
      }
    } catch (ParseException x) {
      throw new RuntimeException(x);
    }
  }

  @SuppressWarnings("EmptyForIteratorPad")
  public static GeometryCollection toGeometryCollection(final @Nonnull NullableVarBinaryHolder holder) {
    try {
      ArrowBuf buffer = holder.buffer;
      WKBReader reader = new WKBReader();
      List<Geometry> geometries = Lists.newLinkedList();
      for (long index = 0L; index < buffer.readableBytes(); ) {
        int size = buffer.getInt(index);
        index += Integer.BYTES;
        byte[] array = new byte[size];
        buffer.getBytes(index, array);
        index += size;
        geometries.add(reader.read(array));
      }
      return GeometryCollections.collect(geometries.toArray(new Geometry[0]));
    } catch (ParseException x) {
      throw new RuntimeException(x);
    }
  }

  @Nonnull
  public static Point toPoint(
      final @Nonnull NullableVarBinaryHolder holder) {
    return (Point) toGeometry(holder);
  }

  @Nonnull
  public static LineString toLineString(
      final @Nonnull NullableVarBinaryHolder holder) {
    return (LineString) toGeometry(holder);
  }

  public static double toAngleRadians(
      final @Nonnull Point s1,
      final @Nonnull Point e1,
      final @Nonnull Point s2,
      final @Nonnull Point e2) {
    double a1 = Angle.angle(s1.getCoordinate(), e1.getCoordinate());
    double a2 = Angle.angle(s2.getCoordinate(), e2.getCoordinate());
    return Angle.normalizePositive(a1 - a2);
  }


  public static double toAzimuthRadians(
      final @Nonnull Point p1,
      final @Nonnull Point p2) {
    double a = Angle.angle(p1.getCoordinate(), p2.getCoordinate());
    return Angle.normalizePositive(AZIMUTH_NORTH_RADIANS - a);
  }

  @Nonnull
  public static Geometry toGeometry(
      final @Nonnull NullableVarBinaryHolder holder) {
    if (holder.buffer != null) {
      ByteBuffer buffer = holder.buffer.nioBuffer(holder.start, holder.end - holder.start);
      try (InputStream stream = ByteBufferInputStream.toInputStream(buffer)) {
        InputStreamInStream adapter = new InputStreamInStream(stream);
        WKBReader reader = new WKBReader();
        return reader.read(adapter);
      } catch (IOException | ParseException x) {
        throw new RuntimeException(x);
      }
    } else {
      return emptyGeometry();
    }
  }

  public static int toBitValue(final boolean value) {
    return value ? BIT_TRUE : BIT_FALSE;
  }

  public static void populate(
      final @Nonnull byte[] bytes,
      final @Nonnull ArrowBuf buffer,
      final @Nonnull NullableVarCharHolder holder) {
    holder.isSet = BIT_TRUE;
    holder.buffer = buffer;
    holder.start = 0;
    holder.end = bytes.length;
    holder.buffer.setBytes(holder.start, bytes);
  }

  public static void populate(
      final @Nonnull byte[] bytes,
      final @Nonnull ArrowBuf buffer,
      final @Nonnull NullableVarBinaryHolder holder) {
    holder.isSet = BIT_TRUE;
    holder.buffer = buffer;
    holder.start = 0;
    holder.end = bytes.length;
    holder.buffer.setBytes(holder.start, bytes);
  }

  public static void append(
      final @Nonnull byte[] bytes,
      final @Nonnull ArrowBuf buffer,
      final @Nonnull NullableVarBinaryHolder holder) {
    holder.buffer = buffer;
    holder.end += bytes.length;
    holder.buffer.writeInt(bytes.length);
    holder.buffer.writeBytes(bytes);
  }

  public static boolean isClosed(
      final @Nullable Geometry geometry) {
    if (geometry == null) {
      return false;
    } else if (geometry.getGeometryType().equals(Geometry.TYPENAME_LINESTRING)) {
      return ((LineString) geometry).isClosed();
    } else if (geometry.getGeometryType().equals(Geometry.TYPENAME_POINT)) {
      return true;
    } else if (geometry.getGeometryType().equals(Geometry.TYPENAME_POLYGON)) {
      return ((Polygon) geometry).getExteriorRing().isClosed();
    } else if (geometry.getGeometryType().equals(Geometry.TYPENAME_LINEARRING)) {
      return ((LinearRing) geometry).isClosed();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported geometry type - %s",
              geometry.getGeometryType()));
    }
  }

  public static boolean isAPoint(
      final @Nullable Geometry geometry) {
    return geometry != null && geometry.getGeometryType().equals(Geometry.TYPENAME_POINT);
  }

  public static boolean isAreal(
      final @Nullable Geometry geometry) {
    return geometry != null && AREAL_TYPES.contains(geometry.getGeometryType());
  }

  public static boolean isACollection(
      final @Nullable Geometry geometry) {
    return geometry instanceof GeometryCollection;
  }

  public static boolean isLinear(
      final @Nullable Geometry geometry) {
    return geometry != null && LINEAR_TYPES.contains(geometry.getGeometryType());
  }

  public static boolean areHoldersSet(final @Nonnull ValueHolder... holders) {
    boolean result = false;
    for (ValueHolder holder : holders) {
      result = isHolderSet(holder);
      if (!result) {
        break;
      }
    }
    return result;
  }

  public static boolean isHolderSet(final @Nonnull ValueHolder holder) {
    if (holder instanceof NullableIntHolder) {
      return ((NullableIntHolder) holder).isSet == BIT_TRUE;
    } else if (holder instanceof NullableBitHolder) {
      return ((NullableBitHolder) holder).isSet == BIT_TRUE;
    } else if (holder instanceof NullableVarCharHolder) {
      return ((NullableVarCharHolder) holder).isSet == BIT_TRUE;
    } else if (holder instanceof NullableVarBinaryHolder) {
      return ((NullableVarBinaryHolder) holder).isSet == BIT_TRUE;
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  public static void markHolderSet(final @Nonnull ValueHolder holder) {
    if (holder instanceof NullableIntHolder) {
      ((NullableIntHolder) holder).isSet = BIT_TRUE;
    } else if (holder instanceof NullableBitHolder) {
      ((NullableBitHolder) holder).isSet = BIT_TRUE;
    } else if (holder instanceof NullableVarBinaryHolder) {
      ((NullableVarBinaryHolder) holder).isSet = BIT_TRUE;
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  public static void markHolderNotSet(final @Nonnull ValueHolder holder) {
    if (holder instanceof NullableIntHolder) {
      ((NullableIntHolder) holder).isSet = BIT_FALSE;
    } else if (holder instanceof NullableBitHolder) {
      ((NullableBitHolder) holder).isSet = BIT_FALSE;
    } else if (holder instanceof NullableVarCharHolder) {
      ((NullableVarCharHolder) holder).isSet = BIT_FALSE;
    } else if (holder instanceof NullableVarBinaryHolder) {
      ((NullableVarBinaryHolder) holder).isSet = BIT_FALSE;
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  public static boolean getBooleanValue(final @Nonnull ValueHolder holder) {
    if (holder instanceof BitHolder) {
      return ((BitHolder) holder).value == BIT_TRUE;
    } else if (holder instanceof NullableBitHolder) {
      NullableBitHolder nullable = (NullableBitHolder) holder;
      if (nullable.isSet == BIT_TRUE) {
        return ((NullableBitHolder) holder).value == BIT_TRUE;
      } else {
        throw new IllegalStateException("Cannot verify state of a not-set nullable bit holder");
      }
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  public static void setBooleanValue(final @Nonnull ValueHolder holder, final boolean value) {
    int bitValue = value ? BIT_TRUE : BIT_FALSE;
    if (holder instanceof BitHolder) {
      ((BitHolder) holder).value = bitValue;
    } else if (holder instanceof NullableBitHolder) {
      NullableBitHolder nullable = (NullableBitHolder) holder;
      nullable.value = bitValue;
      nullable.isSet = BIT_TRUE;
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  public static void setDoubleValue(final @Nonnull ValueHolder holder, final double value) {
    if (holder instanceof Float8Holder) {
      ((Float8Holder) holder).value = value;
    } else if (holder instanceof NullableFloat8Holder) {
      NullableFloat8Holder nullable = (NullableFloat8Holder) holder;
      nullable.value = value;
      nullable.isSet = BIT_TRUE;
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  public static ArrowBuf enlargeBufferIfNeeded(
      final @Nonnull ArrowBuf buffer,
      final long required) {
    try {
      if (required > buffer.capacity()) {
        ByteBufferInputStream stream = ByteBufferInputStream.toInputStream(buffer.nioBuffer());
        byte[] data = IOUtils.toByteArray(stream);
        ArrowBuf reallocated = buffer.reallocIfNeeded(required);
        reallocated.writeBytes(data);
        return reallocated;
      } else {
        return buffer;
      }
    } catch (IOException iox) {
      throw new RuntimeException("Could not read existing buffer data", iox);
    }
  }

  public static Geometry buffer(
      final @Nonnull Geometry geometry,
      final double radius,
      final @Nullable String parameters) {
    if (parameters == null || parameters.isEmpty()) {
      return geometry.buffer(radius);
    } else {
      GeometryBufferParameters.Definition definition = GeometryBufferParameters.parse(parameters);
      GeometryBufferParameters.Value.Sides side = (GeometryBufferParameters.Value.Sides) definition
          .setting(GeometryBufferParameters.Parameters.SIDE)
          .orElse(GeometryBufferParameters.Value.Sides.LEFT);
      double sidedRadius = radius * (side.equals(GeometryBufferParameters.Value.Sides.RIGHT) ? -1 : 1);
      return BufferOp.bufferOp(geometry, sidedRadius, definition.parameters());
    }
  }
}