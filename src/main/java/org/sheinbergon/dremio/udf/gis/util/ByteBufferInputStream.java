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

final class ByteBufferInputStream extends java.io.InputStream {

  static ByteBufferInputStream toInputStream(final @Nonnull java.nio.ByteBuffer buffer) {
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