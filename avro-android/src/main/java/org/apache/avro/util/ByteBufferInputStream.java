/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.util;

import java.io.InputStream;
import java.nio.ByteBuffer;

/** Utility to present {@link ByteBuffer} data as an {@link InputStream}. */
public class ByteBufferInputStream extends InputStream {
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
  private final ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  /**
   * @see InputStream#read()
   */
  @Override
  public int read() {
    if (!buffer.hasRemaining()) {
      return -1;
    }
    return buffer.get() & 0xff;
  }

  /**
   * @see InputStream#read(byte[], int, int)
   */
  @Override
  public int read(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int remaining = Math.min(buffer.remaining(), len);
    if (remaining == 0) {
      return -1;
    }
    buffer.get(b, off, remaining);
    return remaining;
  }

  /**
   * Read a buffer from the input without copying, if possible.
   */
  public ByteBuffer readBuffer(int length) {
    if (length == 0)
      return EMPTY_BUFFER;
    if (!buffer.hasRemaining()) {
      return EMPTY_BUFFER;
    }
    if (buffer.remaining() == length) { // can return current as-is?
      return buffer; // return w/o copying
    }
    // punt: allocate a new buffer & copy into it
    ByteBuffer result = ByteBuffer.allocate(length);
    int start = 0;
    while (start < length)
      start += read(result.array(), start, length - start);
    return result;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      // n may be negative and results in skipping 0 bytes, according to javadoc
      return 0;
    }
    // this catches n > Integer.MAX_VALUE
    int bytesToSkip = n > buffer.remaining() ? buffer.remaining() : (int) n;
    buffer.position(buffer.position() + bytesToSkip);
    return bytesToSkip;
  }
}
