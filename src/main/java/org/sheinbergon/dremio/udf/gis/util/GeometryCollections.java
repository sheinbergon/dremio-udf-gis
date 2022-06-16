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

import javax.annotation.Nonnull;

public final class GeometryCollections {

  public static org.locationtech.jts.geom.GeometryCollection empty() {
    org.locationtech.jts.geom.GeometryFactory factory = new org.locationtech.jts.geom.GeometryFactory();
    return factory.createGeometryCollection();
  }

  public static org.locationtech.jts.geom.GeometryCollection collect(final org.locationtech.jts.geom.Geometry... geometries) {
    org.locationtech.jts.geom.GeometryFactory factory = new org.locationtech.jts.geom.GeometryFactory();
    java.util.List<Class<? extends org.locationtech.jts.geom.Geometry>> types = geometryTypes(geometries);
    if (types.isEmpty()) {
      return geometryCollection(factory);
    } else if (types.size() == 1) {
      java.lang.Class<? extends org.locationtech.jts.geom.Geometry> type = types.get(0);
      if (type == org.locationtech.jts.geom.LineString.class) {
        return multiLineString(factory, geometries);
      } else if (type == org.locationtech.jts.geom.Point.class) {
        return multiPoint(factory, geometries);
      } else if (type == org.locationtech.jts.geom.Polygon.class) {
        return multiPolygon(factory, geometries);
      } else {
        return geometryCollection(factory, geometries);
      }
    } else {
      return geometryCollection(factory, geometries);
    }
  }

  private static java.util.List<Class<? extends org.locationtech.jts.geom.Geometry>> geometryTypes(
      final @Nonnull org.locationtech.jts.geom.Geometry[] geometries) {
    return java.util.stream.Stream
        .of(geometries)
        .map(geometry -> geometry.getClass())
        .distinct()
        .collect(java.util.stream.Collectors.toList());
  }

  private static org.locationtech.jts.geom.MultiLineString multiLineString(
      final @Nonnull org.locationtech.jts.geom.GeometryFactory factory,
      final @Nonnull org.locationtech.jts.geom.Geometry[] geometries) {
    org.locationtech.jts.geom.LineString[] lineStrings = java.util.stream.Stream.of(geometries)
        .map(geometry -> org.locationtech.jts.geom.LineString.class.cast(geometry))
        .toArray(size -> new org.locationtech.jts.geom.LineString[size]);
    return factory.createMultiLineString(lineStrings);
  }

  private static org.locationtech.jts.geom.MultiPoint multiPoint(
      final @Nonnull org.locationtech.jts.geom.GeometryFactory factory,
      final @Nonnull org.locationtech.jts.geom.Geometry[] geometries) {
    org.locationtech.jts.geom.Point[] points = java.util.stream.Stream.of(geometries)
        .map(geometry -> org.locationtech.jts.geom.Point.class.cast(geometry))
        .toArray(size -> new org.locationtech.jts.geom.Point[size]);
    return factory.createMultiPoint(points);
  }

  private static org.locationtech.jts.geom.MultiPolygon multiPolygon(
      final @Nonnull org.locationtech.jts.geom.GeometryFactory factory,
      final @Nonnull org.locationtech.jts.geom.Geometry[] geometries) {
    org.locationtech.jts.geom.Polygon[] polygons = java.util.stream.Stream.of(geometries)
        .map(geometry -> org.locationtech.jts.geom.Polygon.class.cast(geometry))
        .toArray(size -> new org.locationtech.jts.geom.Polygon[size]);
    return factory.createMultiPolygon(polygons);
  }

  private static org.locationtech.jts.geom.GeometryCollection geometryCollection(
      final @Nonnull org.locationtech.jts.geom.GeometryFactory factory,
      final @Nonnull org.locationtech.jts.geom.Geometry[] geometries) {
    return factory.createGeometryCollection(geometries);
  }

  private static org.locationtech.jts.geom.GeometryCollection geometryCollection(
      final @Nonnull org.locationtech.jts.geom.GeometryFactory factory) {
    return factory.createGeometryCollection();
  }

  private GeometryCollections() {
  }

}