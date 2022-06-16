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


import org.locationtech.jts.geom.Geometry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class GeometryHelpers {

  public static final int BIT_TRUE = 1;
  public static final int BIT_FALSE = 0;
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


  public static org.locationtech.jts.geom.GeometryCollection addToGeometryCollection(
      final @Nonnull org.locationtech.jts.geom.GeometryCollection collection,
      final @Nonnull org.locationtech.jts.geom.Geometry addition) {
    org.locationtech.jts.geom.Geometry[] geometries = java.util.stream.Stream.concat(
        java.util.stream.IntStream
            .range(0, collection.getNumGeometries())
            .mapToObj(index -> collection.getGeometryN(index)),
        java.util.stream.Stream.of(addition)
    ).toArray(size -> new Geometry[size]);
    return org.sheinbergon.dremio.udf.gis.util.GeometryCollections.collect(geometries);
  }

  public static org.locationtech.jts.geom.GeometryCollection toGeometryCollection(
      final @Nonnull org.apache.arrow.vector.holders.NullableVarBinaryHolder holder) {
    return (org.locationtech.jts.geom.GeometryCollection) toGeometry(holder);
  }

  public static org.locationtech.jts.geom.Geometry toGeometry(
      final @Nonnull org.apache.arrow.vector.holders.NullableVarBinaryHolder holder) {
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

  public static boolean isAPoint(
      final @Nullable org.locationtech.jts.geom.Geometry geometry) {
    return geometry != null && geometry.getGeometryType().equals(POINT);
  }

  public static boolean isHolderSet(final @Nonnull org.apache.arrow.vector.holders.ValueHolder holder) {
    if (holder instanceof org.apache.arrow.vector.holders.NullableIntHolder) {
      return ((org.apache.arrow.vector.holders.NullableIntHolder) holder).isSet == BIT_TRUE;
    } else if (holder instanceof org.apache.arrow.vector.holders.NullableBitHolder) {
      return ((org.apache.arrow.vector.holders.NullableBitHolder) holder).isSet == BIT_TRUE;
    } else if (holder instanceof org.apache.arrow.vector.holders.NullableVarBinaryHolder) {
      return ((org.apache.arrow.vector.holders.NullableVarBinaryHolder) holder).isSet == BIT_TRUE;
    } else {
      throw new java.lang.IllegalArgumentException(
          java.lang.String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  public static void markHolderSet(final @Nonnull org.apache.arrow.vector.holders.ValueHolder holder) {
    if (holder instanceof org.apache.arrow.vector.holders.NullableIntHolder) {
      ((org.apache.arrow.vector.holders.NullableIntHolder) holder).isSet = BIT_TRUE;
    } else if (holder instanceof org.apache.arrow.vector.holders.NullableBitHolder) {
      ((org.apache.arrow.vector.holders.NullableBitHolder) holder).isSet = BIT_TRUE;
    } else if (holder instanceof org.apache.arrow.vector.holders.NullableVarBinaryHolder) {
      ((org.apache.arrow.vector.holders.NullableVarBinaryHolder) holder).isSet = BIT_TRUE;
    } else {
      throw new java.lang.IllegalArgumentException(
          java.lang.String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  public static void markHolderNotSet(final @Nonnull org.apache.arrow.vector.holders.ValueHolder holder) {
    if (holder instanceof org.apache.arrow.vector.holders.NullableIntHolder) {
      ((org.apache.arrow.vector.holders.NullableIntHolder) holder).isSet = BIT_FALSE;
    } else if (holder instanceof org.apache.arrow.vector.holders.NullableBitHolder) {
      ((org.apache.arrow.vector.holders.NullableBitHolder) holder).isSet = BIT_FALSE;
    } else if (holder instanceof org.apache.arrow.vector.holders.NullableVarBinaryHolder) {
      ((org.apache.arrow.vector.holders.NullableVarBinaryHolder) holder).isSet = BIT_FALSE;
    } else {
      throw new java.lang.IllegalArgumentException(
          java.lang.String.format("Unsupported value holder type - %s",
              holder.getClass().getName()));
    }
  }

  private GeometryHelpers() {
  }
}
