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

import javax.annotation.Nonnull;

public final class GeometryTransformation {


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
    if (geom instanceof Polygon) {
      return transform(transform, (Polygon) geom);
    } else if (geom instanceof Point) {
      return transform(transform, (Point) geom);
    } else if (geom instanceof LinearRing) {
      return transform(transform, (LinearRing) geom);
    } else if (geom instanceof LineString) {
      return transform(transform, (LineString) geom);
    } else if (geom instanceof MultiPolygon) {
      return transform(transform, (MultiPolygon) geom);
    } else if (geom instanceof MultiPoint) {
      return transform(transform, (MultiPoint) geom);
    } else if (geom instanceof MultiLineString) {
      return transform(transform, (MultiLineString) geom);
    } else if (geom instanceof GeometryCollection) {
      return transform(transform, (GeometryCollection) geom);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported geometry type for conversion - %s",
              geom.getGeometryType()));
    }
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
    return polygon.getFactory().createPolygon(
        transformCoordinates(
            transform, polygon.getCoordinates()));
  }

  @Nonnull
  private static Geometry transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final Point point) {
    return point.getFactory().createPoint(
        transformCoordinates(
            transform, point.getCoordinates())[0]);
  }

  @Nonnull
  private static Geometry transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final LinearRing linearRing) {
    return linearRing.getFactory()
        .createLinearRing(
            transformCoordinates(transform, linearRing.getCoordinates()));
  }

  @Nonnull
  private static Geometry transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final LineString lineString) {
    return lineString.getFactory().createLineString(
        transformCoordinates(
            transform, lineString.getCoordinates()));
  }

  @Nonnull
  private static Geometry transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final MultiPolygon multiPolygon) {
    Polygon[] polygon = new Polygon[multiPolygon.getNumGeometries()];
    for (int i = 0; i < polygon.length; ++i) {
      polygon[i] = multiPolygon.getFactory()
          .createPolygon(transformCoordinates(transform,
              multiPolygon.getGeometryN(i).getCoordinates()));
    }
    return multiPolygon.getFactory().createMultiPolygon(polygon);
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
  private static Geometry transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final MultiLineString multiLineString) {
    LineString[] lineString = new LineString[multiLineString.getNumGeometries()];
    for (int index = 0; index < lineString.length; ++index) {
      lineString[index] = multiLineString.getFactory()
          .createLineString(transformCoordinates(transform,
              multiLineString.getGeometryN(index).getCoordinates()));
    }
    return multiLineString.getFactory().createMultiLineString(lineString);
  }

  @Nonnull
  private static Geometry transform(
      @Nonnull final CoordinateTransform transform,
      @Nonnull final GeometryCollection collection) {
    Geometry[] geometry = new Geometry[collection.getNumGeometries()];
    for (int index = 0; index < geometry.length; ++index) {
      geometry[index] = transform(collection.getGeometryN(index), transform);
    }
    return collection.getFactory().createGeometryCollection(geometry);
  }
}