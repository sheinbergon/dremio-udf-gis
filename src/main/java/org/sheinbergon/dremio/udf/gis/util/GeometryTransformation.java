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

// Inspired by https://github.com/teiid/teiid/blob/master/optional-geo/src/main/java/org/teiid/geo/GeometryTransformUtils.java

import org.locationtech.jts.geom.*;
import org.locationtech.proj4j.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.stream.IntStream;

public final class GeometryTransformation {

  private static final Logger logger = LoggerFactory.getLogger(GeometryTransformation.class);

  private static final String CRS_TEMPLATE = "EPSG:%d";

  private GeometryTransformation() {
  }

  public static Geometry transform(
      final @Nonnull Geometry geom,
      final int targetSrid) {
    CRSFactory factory = new CRSFactory();
    CoordinateReferenceSystem source = factory.createFromName(String.format(CRS_TEMPLATE, geom.getSRID()));
    CoordinateReferenceSystem target = factory.createFromName(String.format(CRS_TEMPLATE, targetSrid));
    return transform(geom, source, target);
  }

  public static Geometry transform(
      final @Nonnull Geometry geom,
      final @Nonnull String targetProj4Parameters) {
    CRSFactory factory = new CRSFactory();
    CoordinateReferenceSystem source = factory.createFromName(String.format(CRS_TEMPLATE, geom.getSRID()));
    CoordinateReferenceSystem target = factory.createFromParameters(null, targetProj4Parameters);
    return transform(geom, source, target);
  }

  public static Geometry transform(
      final @Nonnull Geometry geom,
      final @Nonnull String sourceProj4Parameters,
      final int targetSrid) {
    CRSFactory factory = new CRSFactory();
    CoordinateReferenceSystem source = factory.createFromParameters(null, sourceProj4Parameters);
    CoordinateReferenceSystem target = factory.createFromName(String.format(CRS_TEMPLATE, targetSrid));
    return transform(geom, source, target);
  }

  private static Geometry transform(
      final @Nonnull Geometry geom,
      final @Nonnull CoordinateReferenceSystem source,
      final @Nonnull CoordinateReferenceSystem target) {
    CoordinateTransform transform = new BasicCoordinateTransform(source, target);
    return transform(geom, transform);
  }

  private static Geometry transform(
      final @Nonnull Geometry geom,
      final @Nonnull CoordinateTransform transform) {
    Geometry transformed = null;
    if (geom instanceof Polygon) {
      transformed = transform(transform, (Polygon) geom);
    } else if (geom instanceof Point) {
      transformed = transform(transform, (Point) geom);
    } else if (geom instanceof LineString) {
      transformed = transform(transform, (LineString) geom);
    } else if (geom instanceof MultiPolygon) {
      transformed = transform(transform, (MultiPolygon) geom);
    } else if (geom instanceof MultiPoint) {
      transformed = transform(transform, (MultiPoint) geom);
    } else if (geom instanceof MultiLineString) {
      transformed = transform(transform, (MultiLineString) geom);
    } else if (geom instanceof GeometryCollection) {
      transformed = transform(transform, (GeometryCollection) geom);
    } else if (geom instanceof LinearRing) {
      transformed = transform(transform, (LinearRing) geom);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported geometry type for conversion - %s",
              geom.getGeometryType()));
    }
    return transformed;
  }

  private static Coordinate[] convert(
      final @Nonnull ProjCoordinate[] projCoordinates) {
    Coordinate[] jtsCoordinates = new Coordinate[projCoordinates.length];
    for (int i = 0; i < projCoordinates.length; ++i) {
      jtsCoordinates[i] = new Coordinate(projCoordinates[i].x, projCoordinates[i].y);
    }
    return jtsCoordinates;
  }

  private static ProjCoordinate[] convert(
      final @Nonnull Coordinate[] jtsCoordinates) {
    ProjCoordinate[] projectionCoordinates = new ProjCoordinate[jtsCoordinates.length];
    for (int i = 0; i < jtsCoordinates.length; ++i) {
      projectionCoordinates[i] = new ProjCoordinate(jtsCoordinates[i].x, jtsCoordinates[i].y);
    }
    return projectionCoordinates;
  }

  static Coordinate[] transformCoordinates(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final Coordinate[] source) {
    return convert(transformCoordinates(transform, convert(source)));
  }

  @Nonnull
  private static ProjCoordinate[] transformCoordinates(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final ProjCoordinate[] source) {
    ProjCoordinate[] out = new ProjCoordinate[source.length];
    for (int index = 0; index < source.length; ++index) {
      out[index] = transform.transform(source[index], new ProjCoordinate());
    }
    return out;
  }

  @Nonnull
  private static Polygon transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final Polygon polygon) {
    LinearRing exterior = transform(transform, polygon.getExteriorRing());
    LinearRing[] interior = IntStream.range(0, polygon.getNumInteriorRing())
        .mapToObj(polygon::getInteriorRingN)
        .map(ring -> transform(transform, ring))
        .toArray(LinearRing[]::new);
    return polygon.getFactory().createPolygon(exterior, interior);
  }

  @Nonnull
  private static Point transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final Point point) {
    return point.getFactory().createPoint(
        transformCoordinates(
            transform, point.getCoordinates())[0]);
  }

  @Nonnull
  private static LinearRing transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final LinearRing linearRing) {
    return linearRing.getFactory()
        .createLinearRing(
            transformCoordinates(transform, linearRing.getCoordinates()));
  }

  @Nonnull
  private static LineString transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final LineString lineString) {
    return lineString.getFactory().createLineString(
        transformCoordinates(
            transform, lineString.getCoordinates()));
  }

  @Nonnull
  private static MultiPolygon transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final MultiPolygon multiPolygon) {
    Polygon[] polygons = new Polygon[multiPolygon.getNumGeometries()];
    Arrays.setAll(polygons, i -> transform(
        transform,
        (Polygon) multiPolygon.getGeometryN(i)));
    return multiPolygon.getFactory().createMultiPolygon(polygons);
  }

  @Nonnull
  private static Geometry transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final MultiPoint multiPoint) {
    return multiPoint.getFactory().createMultiPointFromCoords(
        transformCoordinates(
            transform, multiPoint.getCoordinates()));
  }

  @Nonnull
  private static MultiLineString transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final MultiLineString multiLineString) {
    LineString[] lineString = new LineString[multiLineString.getNumGeometries()];
    Arrays.setAll(lineString, index -> transform(
        transform,
        (LineString) multiLineString.getGeometryN(index)));
    return multiLineString.getFactory().createMultiLineString(lineString);
  }

  @Nonnull
  private static GeometryCollection transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final GeometryCollection collection) {
    Geometry[] geometry = new Geometry[collection.getNumGeometries()];
    for (int index = 0; index < geometry.length; ++index) {
      geometry[index] = transform(collection.getGeometryN(index), transform);
    }
    return collection.getFactory().createGeometryCollection(geometry);
  }
}