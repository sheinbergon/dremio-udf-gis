package org.sheinbergon.dremio.udf.gis.util;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.GeometryFixer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

public final class GeometryReparation {

  private static final String KV_DELIMITER = "=";

  private static final String PAIR_DELIMITER = "\\s+";

  interface Value {
    final class BooleanValue implements Value {

      public static BooleanValue parse(final @Nonnull String string) {
        return new BooleanValue(Boolean.parseBoolean(string));
      }

      final boolean value;

      private BooleanValue(final boolean value) {
        this.value = value;
      }
    }
  }

  public enum Parameters {
    KEEPCOLLAPSED {
      @Override
      void process(
          @Nonnull final String value,
          @Nonnull final GeometryFixer instance) {
        Value.BooleanValue b = Value.BooleanValue.parse(value);
        instance.setKeepCollapsed(b.value);
      }
    };

    public static Parameters valueFrom(final @Nonnull String value) {
      try {
        return Parameters.valueOf(value.toUpperCase());
      } catch (IllegalArgumentException iax) {
        throw new IllegalArgumentException(
            String.format("Invalid parameter name '%s'",
                value));
      }
    }

    abstract void process(@Nonnull String value, @Nonnull GeometryFixer instance);
  }

  private static GeometryFixer instance(final @Nonnull Geometry geometry) {
    GeometryFixer instance = new GeometryFixer(geometry);
    instance.setKeepMulti(true);
    return instance;
  }

  public static Geometry repair(
      final @Nonnull Geometry geometry,
      final @Nullable String string) {
    final GeometryFixer instance = instance(geometry);
    if (string != null) {
      Arrays.stream(string.split(PAIR_DELIMITER))
          .forEach(pair -> {
            String[] parts = pair.split(KV_DELIMITER);
            Parameters parameter = Parameters.valueFrom(parts[0]);
            parameter.process(parts[1], instance);
          });
    }
    return instance.getResult();
  }

  private GeometryReparation() {
  }
}
