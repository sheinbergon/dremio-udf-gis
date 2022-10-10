package org.sheinbergon.dremio.udf.gis.util;

import org.locationtech.jts.operation.buffer.BufferParameters;
import org.locationtech.proj4j.util.Pair;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class GeometryBufferParameters {

  private static final String KV_DELIMITER = "=";

  private static final String PAIR_DELIMITER = "\\s+";

  interface Value<V> {
    enum Sides implements Value<Sides> {
      LEFT, RIGHT, BOTH;

      public Sides value() {
        return this;
      }
    }

    enum JoinStyles implements Value<JoinStyles> {
      BEVEL(BufferParameters.JOIN_BEVEL),
      MITRE(BufferParameters.JOIN_MITRE),
      ROUND(BufferParameters.JOIN_ROUND);

      final int value;

      JoinStyles(final int value) {
        this.value = value;
      }

      public JoinStyles value() {
        return this;
      }
    }

    enum CapStyles implements Value<CapStyles> {
      FLAT(BufferParameters.CAP_FLAT),
      ROUND(BufferParameters.CAP_ROUND),
      SQUARE(BufferParameters.CAP_SQUARE);

      final int value;

      CapStyles(final int value) {
        this.value = value;
      }

      public CapStyles value() {
        return this;
      }
    }

    final class IntValue implements Value<Integer> {

      public static Value.IntValue parse(final @Nonnull String string) {
        return new IntValue(Integer.parseInt(string));
      }

      final int value;

      private IntValue(final int value) {
        this.value = value;
      }

      public Integer value() {
        return value;
      }
    }

    final class DoubleValue implements Value<Double> {

      public static Value.DoubleValue parse(final @Nonnull String string) {
        return new DoubleValue(Double.parseDouble(string));
      }

      final double value;

      private DoubleValue(final double value) {
        this.value = value;
      }

      public Double value() {
        return value;
      }
    }

    V value();
  }

  public enum Parameters {
    QUAD_SEGS {
      @Override
      Value.IntValue process(
          @Nonnull final String value,
          @Nonnull final BufferParameters instance) {
        Value.IntValue i = Value.IntValue.parse(value);
        instance.setQuadrantSegments(i.value);
        return i;
      }
    },
    ENDCAP {
      @Override
      Value.CapStyles process(
          @Nonnull final String value,
          @Nonnull final BufferParameters instance) {
        Value.CapStyles style = Value.CapStyles.valueOf(value.toUpperCase());
        instance.setEndCapStyle(style.value);
        return style;
      }
    },
    JOIN {
      Value.JoinStyles process(
          @Nonnull final String value,
          @Nonnull final BufferParameters instance) {
        Value.JoinStyles style = Value.JoinStyles.valueOf(value.toUpperCase());
        instance.setJoinStyle(style.value);
        return style;
      }
    },
    MITRE_LIMIT {
      @Override
      Value.DoubleValue process(
          @Nonnull final String value,
          @Nonnull final BufferParameters instance) {
        Value.DoubleValue d = Value.DoubleValue.parse(value);
        instance.setMitreLimit(d.value);
        return d;
      }
    },
    SIDE {
      @Override
      Value.Sides process(
          @Nonnull final String value,
          @Nonnull final BufferParameters instance) {
        final Value.Sides side = (Value.Sides.valueOf(value.toUpperCase()));
        instance.setSingleSided(side.equals(Value.Sides.BOTH));
        return side;
      }
    };

    abstract Value<?> process(@Nonnull String value, @Nonnull BufferParameters instance);
  }

  public static Definition parse(final @Nonnull String string) {
    final BufferParameters instance = new BufferParameters();
    final Map<Parameters, Value<?>> settings = Arrays
        .stream(string.split(PAIR_DELIMITER))
        .map(kvpair -> {
          String[] pair = kvpair.split(KV_DELIMITER);
          Parameters parameter = Parameters.valueOf(pair[0].toUpperCase());
          Value<?> value = parameter.process(pair[1], instance);
          return Pair.create(parameter, value);
        }).collect(Collectors.toMap(Pair::fst, Pair::snd));
    return Definition.wrap(instance, settings);
  }

  private GeometryBufferParameters() {
  }

  public static final class Definition {

    public static Definition wrap(
        final BufferParameters parameters,
        final Map<Parameters, Value<?>> settings) {
      return new Definition(parameters, settings);
    }

    private final BufferParameters parameters;
    private final Map<Parameters, Value<?>> settings;

    public BufferParameters parameters() {
      return parameters;
    }

    public Optional<Value<?>> setting(final @Nonnull Parameters parameter) {
      return Optional.ofNullable(settings.get(parameter));
    }

    private Definition(
        final BufferParameters parameters,
        final Map<Parameters, Value<?>> settings) {
      this.parameters = parameters;
      this.settings = settings;
    }
  }
}
