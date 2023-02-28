package org.sheinbergon.dremio.udf.gis.util;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.operation.valid.TopologyValidationError;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class GeometryValidationResult {

  private static final GeometryValidationResult VALID = new GeometryValidationResult(true, "Valid Geometry", null);

  static GeometryValidationResult valid() {
    return VALID;
  }

  static GeometryValidationResult invalid(final @Nonnull TopologyValidationError error) {
    return new GeometryValidationResult(false, error.getMessage(), error.getCoordinate());
  }

  private final boolean valid;
  private final String reason;
  private final Coordinate location;

  private GeometryValidationResult(
      final boolean valid,
      final @Nonnull String reason,
      final @Nullable Coordinate location) {
    this.valid = valid;
    this.reason = reason;
    this.location = location;
  }

  public boolean isValid() {
    return valid;
  }

  public String getFormattedReason() {
    if (valid || location == null) {
      return reason;
    } else {
      return String.format("%s [%s %s]", reason, location.x, location.y);
    }
  }
}
