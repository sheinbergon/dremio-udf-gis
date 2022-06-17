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
import com.google.common.collect.Sets;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.io.*;
import org.locationtech.jts.io.geojson.GeoJsonWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class GeometryHelpers {

  public static final int BIT_TRUE = 1;
  public static final int BIT_FALSE = 0;
  public static final int DEFAULT_SRID = 4326;

  private static final Set<String> AREAL_TYPES = Sets.newHashSet(Geometry.TYPENAME_POLYGON, Geometry.TYPENAME_MULTIPOLYGON);

  private static final Set<String> LINEAR_TYPES = Sets.newHashSet(Geometry.TYPENAME_LINESTRING, Geometry.TYPENAME_MULTILINESTRING);

  private static final int GEOMETRY_WRITER_DIMENSIONS = 2;

  private GeometryHelpers() {
  }

  public static String toUTF8String(final @Nonnull VarCharHolder holder) {
    return StringFunctionHelpers.toStringFromUTF8(
        holder.start,
        holder.end,
        holder.buffer);
  }

  private static String toUTF8String(final @Nonnull NullableVarCharHolder holder) {
    return StringFunctionHelpers.toStringFromUTF8(
        holder.start,
        holder.end,
        holder.buffer);
  }

  public static byte[] toBinary(final @Nonnull Geometry geometry) {
    WKBWriter writer = new WKBWriter(GEOMETRY_WRITER_DIMENSIONS, true);
    return writer.write(geometry);
  }

  public static byte[] toText(
      final @Nonnull Geometry geometry) {
    WKTWriter writer = new WKTWriter(GEOMETRY_WRITER_DIMENSIONS);
    return writer.write(geometry).getBytes(StandardCharsets.UTF_8);
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

  public static GeometryCollection addToGeometryCollection(
      final @Nonnull GeometryCollection collection,
      final @Nonnull Geometry addition) {
    Geometry[] geometries = Stream.concat(
        IntStream
            .range(0, collection.getNumGeometries())
            .mapToObj(index -> collection.getGeometryN(index)),
        Stream.of(addition)
    ).toArray(size -> new Geometry[size]);
    return GeometryCollections.collect(geometries);
  }

  public static GeometryCollection toGeometryCollection(
      final @Nonnull NullableVarBinaryHolder holder) {
    return (GeometryCollection) toGeometry(holder);
  }

  public static Geometry toGeometry(
      final @Nonnull NullableVarBinaryHolder holder) {
    ByteBuffer buffer = holder.buffer.nioBuffer(holder.start, holder.end - holder.start);
    try (InputStream stream = ByteBufferInputStream.toInputStream(buffer)) {
      InputStreamInStream adapter = new InputStreamInStream(stream);
      WKBReader reader = new WKBReader();
      return reader.read(adapter);
    } catch (IOException | ParseException x) {
      throw new RuntimeException(x);
    }
  }

  public static int toBitValue(final boolean value) {
    return value ? BIT_TRUE : BIT_FALSE;
  }

  public static void populate(
      final @Nonnull byte[] bytes,
      final @Nonnull ArrowBuf buffer,
      final @Nonnull NullableVarCharHolder output) {
    output.buffer = buffer;
    output.start = 0;
    output.end = bytes.length;
    output.buffer.setBytes(output.start, bytes);
  }

  public static void populate(
      final @Nonnull byte[] bytes,
      final @Nonnull ArrowBuf buffer,
      final @Nonnull NullableVarBinaryHolder output) {
    output.buffer = buffer;
    output.start = 0;
    output.end = bytes.length;
    output.buffer.setBytes(output.start, bytes);
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

  public static boolean isHolderSet(final @Nonnull ValueHolder holder) {
    if (holder instanceof NullableIntHolder) {
      return ((NullableIntHolder) holder).isSet == BIT_TRUE;
    } else if (holder instanceof NullableBitHolder) {
      return ((NullableBitHolder) holder).isSet == BIT_TRUE;
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
    } else if (holder instanceof NullableVarBinaryHolder) {
      ((NullableVarBinaryHolder) holder).isSet = BIT_FALSE;
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }
}
