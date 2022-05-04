package org.sheinbergon.dremio.udf.gis.util;

import javax.annotation.Nonnull;

final class ByteBufferInputStream extends java.io.InputStream {

  static ByteBufferInputStream toInStream(final @Nonnull java.nio.ByteBuffer buffer) {
    return new ByteBufferInputStream(buffer);
  }

  private final java.nio.ByteBuffer buffer;

  private ByteBufferInputStream(final @Nonnull java.nio.ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  @SuppressWarnings("MagicNumber")
  public int read() {
    if (!buffer.hasRemaining()) {
      return -1;
    }
    return buffer.get() & 0xFF;
  }

  @Override
  public int read(final @Nonnull byte[] bytes, final int off, final int len) {
    if (!buffer.hasRemaining()) {
      return -1;
    }
    int read = Math.min(len, buffer.remaining());
    buffer.get(bytes, off, read);
    return read;
  }
}