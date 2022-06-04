package org.sheinbergon.dremio.udf.gis.util;

// Inspired by https://github.com/teiid/teiid/blob/master/optional-geo/src/main/java/org/teiid/geo/GeometryTransformUtils.java

import javax.annotation.Nonnull;

public final class GeometryTransformation {

  public static org.locationtech.jts.geom.Geometry transform(
      final @Nonnull org.locationtech.jts.geom.Geometry geom,
      final @Nonnull org.locationtech.proj4j.CoordinateTransform transform) {
    if (geom instanceof org.locationtech.jts.geom.Polygon) {
      return transform(transform, (org.locationtech.jts.geom.Polygon) geom);
    } else if (geom instanceof org.locationtech.jts.geom.Point) {
      return transform(transform, (org.locationtech.jts.geom.Point) geom);
    } else if (geom instanceof org.locationtech.jts.geom.LinearRing) {
      return transform(transform, (org.locationtech.jts.geom.LinearRing) geom);
    } else if (geom instanceof org.locationtech.jts.geom.LineString) {
      return transform(transform, (org.locationtech.jts.geom.LineString) geom);
    } else if (geom instanceof org.locationtech.jts.geom.MultiPolygon) {
      return transform(transform, (org.locationtech.jts.geom.MultiPolygon) geom);
    } else if (geom instanceof org.locationtech.jts.geom.MultiPoint) {
      return transform(transform, (org.locationtech.jts.geom.MultiPoint) geom);
    } else if (geom instanceof org.locationtech.jts.geom.MultiLineString) {
      return transform(transform, (org.locationtech.jts.geom.MultiLineString) geom);
    } else if (geom instanceof org.locationtech.jts.geom.GeometryCollection) {
      return transform(transform, (org.locationtech.jts.geom.GeometryCollection) geom);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported geometry type for conversion - %s",
              geom.getGeometryType()));
    }
  }

  private static org.locationtech.jts.geom.Coordinate[] convert(
      final @Nonnull org.locationtech.proj4j.ProjCoordinate[] projCoordinates) {
    org.locationtech.jts.geom.Coordinate[] jtsCoordinates = new org.locationtech.jts.geom.Coordinate[projCoordinates.length];
    for (int i = 0; i < projCoordinates.length; ++i) {
      jtsCoordinates[i] = new org.locationtech.jts.geom.Coordinate(projCoordinates[i].x, projCoordinates[i].y);
    }
    return jtsCoordinates;
  }

  private static org.locationtech.proj4j.ProjCoordinate[] convert(
      final @Nonnull org.locationtech.jts.geom.Coordinate[] jtsCoordinates) {
    org.locationtech.proj4j.ProjCoordinate[] projectionCoordinates = new org.locationtech.proj4j.ProjCoordinate[jtsCoordinates.length];
    for (int i = 0; i < jtsCoordinates.length; ++i) {
      projectionCoordinates[i] = new org.locationtech.proj4j.ProjCoordinate(jtsCoordinates[i].x, jtsCoordinates[i].y);
    }
    return projectionCoordinates;
  }

  protected static org.locationtech.jts.geom.Coordinate[] transformCoordinates(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.jts.geom.Coordinate[] source) {
    return convert(transformCoordinates(transform, convert(source)));
  }

  @Nonnull
  private static org.locationtech.proj4j.ProjCoordinate[] transformCoordinates(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.proj4j.ProjCoordinate[] source) {
    org.locationtech.proj4j.ProjCoordinate[] out = new org.locationtech.proj4j.ProjCoordinate[source.length];
    for (int index = 0; index < source.length; ++index) {
      out[index] = transform.transform(source[index], new org.locationtech.proj4j.ProjCoordinate());
    }
    return out;
  }

  @Nonnull
  private static org.locationtech.jts.geom.Polygon transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.jts.geom.Polygon polygon) {
    return polygon.getFactory().createPolygon(
        transformCoordinates(
            transform, polygon.getCoordinates()));
  }

  @Nonnull
  private static org.locationtech.jts.geom.Geometry transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.jts.geom.Point point) {
    return point.getFactory().createPoint(
        transformCoordinates(
            transform, point.getCoordinates())[0]);
  }

  @Nonnull
  private static org.locationtech.jts.geom.Geometry transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.jts.geom.LinearRing linearRing) {
    return linearRing.getFactory()
        .createLinearRing(
            transformCoordinates(transform, linearRing.getCoordinates()));
  }

  @Nonnull
  private static org.locationtech.jts.geom.Geometry transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.jts.geom.LineString lineString) {
    return lineString.getFactory().createLineString(
        transformCoordinates(
            transform, lineString.getCoordinates()));
  }

  @Nonnull
  private static org.locationtech.jts.geom.Geometry transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.jts.geom.MultiPolygon multiPolygon) {
    org.locationtech.jts.geom.Polygon[] polygon = new org.locationtech.jts.geom.Polygon[multiPolygon.getNumGeometries()];
    for (int i = 0; i < polygon.length; ++i) {
      polygon[i] = multiPolygon.getFactory()
          .createPolygon(transformCoordinates(transform,
              multiPolygon.getGeometryN(i).getCoordinates()));
    }
    return multiPolygon.getFactory().createMultiPolygon(polygon);
  }

  @Nonnull
  private static org.locationtech.jts.geom.Geometry transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.jts.geom.MultiPoint multiPoint) {
    return multiPoint.getFactory().createMultiPointFromCoords(
        transformCoordinates(
            transform, multiPoint.getCoordinates()));
  }

  @Nonnull
  private static org.locationtech.jts.geom.Geometry transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.jts.geom.MultiLineString multiLineString) {
    org.locationtech.jts.geom.LineString[] lineString = new org.locationtech.jts.geom.LineString[multiLineString.getNumGeometries()];
    for (int index = 0; index < lineString.length; ++index) {
      lineString[index] = multiLineString.getFactory()
          .createLineString(transformCoordinates(transform,
              multiLineString.getGeometryN(index).getCoordinates()));
    }
    return multiLineString.getFactory().createMultiLineString(lineString);
  }

  @Nonnull
  private static org.locationtech.jts.geom.Geometry transform(
      @Nonnull final org.locationtech.proj4j.CoordinateTransform transform,
      @Nonnull final org.locationtech.jts.geom.GeometryCollection collection) {
    org.locationtech.jts.geom.Geometry[] geometry = new org.locationtech.jts.geom.Geometry[collection.getNumGeometries()];
    for (int index = 0; index < geometry.length; ++index) {
      geometry[index] = transform(collection.getGeometryN(index), transform);
    }
    return collection.getFactory().createGeometryCollection(geometry);
  }

  private GeometryTransformation() {
  }
}