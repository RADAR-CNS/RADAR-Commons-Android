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
package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

/**
 * A factory for creating and configuring {@link Encoder} instances.
 * <p/>
 * Factory methods that create Encoder instances are thread-safe. Multiple
 * instances with different configurations can be cached by an application.
 *
 * @see Encoder
 * @see BinaryEncoder
 * @see JsonEncoder
 * @see BufferedBinaryEncoder
 * @see DirectBinaryEncoder
 */

public class EncoderFactory {
  private static final int DEFAULT_BUFFER_SIZE = 2048;

  private static final EncoderFactory DEFAULT_FACTORY = new DefaultEncoderFactory();

  protected int binaryBufferSize = DEFAULT_BUFFER_SIZE;

  /**
   * Returns an immutable static DecoderFactory with default configuration. All
   * configuration methods throw AvroRuntimeExceptions if called.
   */
  public static EncoderFactory get() {
    return DEFAULT_FACTORY;
  }

  /**
   * Configures this factory to use the specified buffer size when creating
   * Encoder instances that buffer their output. The default buffer size is 2048
   * bytes.
   *
   * @param size The buffer size to configure new instances with. Valid values are
   *             in the range [32, 16*1024*1024]. Values outside this range are
   *             set to the nearest value in the range. Values less than 256 will
   *             limit performance but will consume less memory if the
   *             BinaryEncoder is short-lived, values greater than 8*1024 are not
   *             likely to improve performance but may be useful for the
   *             downstream OutputStream.
   * @return This factory, to enable method chaining:
   *
   *         <pre>
   *         EncoderFactory factory = new EncoderFactory().configureBufferSize(4096);
   *         </pre>
   *
   * @see #binaryEncoder(OutputStream, BinaryEncoder)
   */
  public EncoderFactory configureBufferSize(int size) {
    if (size < 32)
      size = 32;
    if (size > 16 * 1024 * 1024)
      size = 16 * 1024 * 1024;
    this.binaryBufferSize = size;
    return this;
  }

  /**
   * Creates or reinitializes a {@link BinaryEncoder} with the OutputStream
   * provided as the destination for written data. If <i>reuse</i> is provided, an
   * attempt will be made to reconfigure <i>reuse</i> rather than construct a new
   * instance, but this is not guaranteed, a new instance may be returned.
   * <p/>
   * The {@link BinaryEncoder} implementation returned may buffer its output. Data
   * may not appear on the underlying OutputStream until {@link Encoder#flush()}
   * is called. The buffer size is configured with
   * {@link #configureBufferSize(int)}.
   * </p>
   * If buffering is not desired, and lower performance is acceptable, use
   * {@link #directBinaryEncoder(OutputStream, BinaryEncoder)}
   * <p/>
   * {@link BinaryEncoder} instances returned by this method are not thread-safe
   *
   * @param out   The OutputStream to write to. Cannot be null.
   * @param reuse The BinaryEncoder to <i>attempt</i> to reuse given the factory
   *              configuration. A BinaryEncoder implementation may not be
   *              compatible with reuse, causing a new instance to be returned. If
   *              null, a new instance is returned.
   * @return A BinaryEncoder that uses <i>out</i> as its data output. If
   *         <i>reuse</i> is null, this will be a new instance. If <i>reuse</i> is
   *         not null, then the returned instance may be a new instance or
   *         <i>reuse</i> reconfigured to use <i>out</i>.
   * @throws IOException
   * @see BufferedBinaryEncoder
   * @see Encoder
   */
  public BinaryEncoder binaryEncoder(OutputStream out, BinaryEncoder reuse) {
    if (null == reuse || !reuse.getClass().equals(BufferedBinaryEncoder.class)) {
      return new BufferedBinaryEncoder(out, this.binaryBufferSize);
    } else {
      return ((BufferedBinaryEncoder) reuse).configure(out, this.binaryBufferSize);
    }
  }

  /**
   * Creates or reinitializes a {@link BinaryEncoder} with the OutputStream
   * provided as the destination for written data. If <i>reuse</i> is provided, an
   * attempt will be made to reconfigure <i>reuse</i> rather than construct a new
   * instance, but this is not guaranteed, a new instance may be returned.
   * <p/>
   * The {@link BinaryEncoder} implementation returned does not buffer its output,
   * calling {@link Encoder#flush()} will simply cause the wrapped OutputStream to
   * be flushed.
   * <p/>
   * Performance of unbuffered writes can be significantly slower than buffered
   * writes. {@link #binaryEncoder(OutputStream, BinaryEncoder)} returns
   * BinaryEncoder instances that are tuned for performance but may buffer output.
   * The unbuffered, 'direct' encoder may be desired when buffering semantics are
   * problematic, or if the lifetime of the encoder is so short that the buffer
   * would not be useful.
   * <p/>
   * {@link BinaryEncoder} instances returned by this method are not thread-safe.
   *
   * @param out   The OutputStream to initialize to. Cannot be null.
   * @param reuse The BinaryEncoder to <i>attempt</i> to reuse given the factory
   *              configuration. A BinaryEncoder implementation may not be
   *              compatible with reuse, causing a new instance to be returned. If
   *              null, a new instance is returned.
   * @return A BinaryEncoder that uses <i>out</i> as its data output. If
   *         <i>reuse</i> is null, this will be a new instance. If <i>reuse</i> is
   *         not null, then the returned instance may be a new instance or
   *         <i>reuse</i> reconfigured to use <i>out</i>.
   * @see DirectBinaryEncoder
   * @see Encoder
   */
  public BinaryEncoder directBinaryEncoder(OutputStream out, BinaryEncoder reuse) {
    if (null == reuse || !reuse.getClass().equals(DirectBinaryEncoder.class)) {
      return new DirectBinaryEncoder(out);
    } else {
      return ((DirectBinaryEncoder) reuse).configure(out);
    }
  }

  /**
   * Creates a {@link JsonEncoder} using the OutputStream provided for writing
   * data conforming to the Schema provided.
   * <p/>
   * {@link JsonEncoder} buffers its output. Data may not appear on the underlying
   * OutputStream until {@link Encoder#flush()} is called.
   * <p/>
   * {@link JsonEncoder} is not thread-safe.
   *
   * @param schema The Schema for data written to this JsonEncoder. Cannot be
   *               null.
   * @param out    The OutputStream to write to. Cannot be null.
   * @return A JsonEncoder configured with <i>out</i> and <i>schema</i>
   */
  public JsonEncoder jsonEncoder(Schema schema, OutputStream out) {
    return new JsonEncoder(schema, out);
  }

  // default encoder is not mutable
  private static class DefaultEncoderFactory extends EncoderFactory {

    @Override
    public EncoderFactory configureBufferSize(int size) {
      throw new AvroRuntimeException("Default EncoderFactory cannot be configured");
    }
  }
}
