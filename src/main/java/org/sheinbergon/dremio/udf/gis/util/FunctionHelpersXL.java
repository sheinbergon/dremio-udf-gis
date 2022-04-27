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

import com.esri.core.geometry.Envelope;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class FunctionHelpersXL {

  static final int BIT_TRUE = 1;
  static final int BIT_FALSE = 0;
  static final String POINT = "Point";
  public static final int DEFAULT_SRID = 4326;


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
      final @Nonnull com.esri.core.geometry.ogc.OGCGeometry geometry) {
    return geometry.asBinary().array();
  }

  public static byte[] toText(
      final @Nonnull com.esri.core.geometry.ogc.OGCGeometry geometry) {
    return geometry.asText().getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }


  public static byte[] toJson(
      final @Nonnull com.esri.core.geometry.ogc.OGCGeometry geometry) {
    return geometry.asJson().getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  public static byte[] toGeoJson(
      final @Nonnull com.esri.core.geometry.ogc.OGCGeometry geometry) {
    return geometry.asGeoJson().getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  public static com.esri.core.geometry.ogc.OGCGeometry toGeometry(
      final @Nonnull org.apache.arrow.vector.holders.NullableVarCharHolder holder) {
    java.lang.String wkt = toUTF8String(holder);
    return com.esri.core.geometry.ogc.OGCGeometry.fromText(wkt);
  }

  public static com.esri.core.geometry.ogc.OGCGeometry toGeometry(
      final @Nonnull org.apache.arrow.vector.holders.NullableVarBinaryHolder holder) {
    var buffer = holder.buffer.nioBuffer(holder.start, holder.end - holder.start);
    return com.esri.core.geometry.ogc.OGCGeometry.fromBinary(buffer);
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
      final @Nullable com.esri.core.geometry.ogc.OGCGeometry geometry,
      final @Nonnull java.util.function.Function<Envelope, Double> getter) {
    var envelope = new com.esri.core.geometry.Envelope();
    geometry.getEsriGeometry().queryEnvelope(envelope);
    return getter.apply(envelope);
  }

  public static boolean isAPoint(
      final @Nullable com.esri.core.geometry.ogc.OGCGeometry geometry) {
    return geometry != null && geometry.geometryType().equals(POINT);
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
}
