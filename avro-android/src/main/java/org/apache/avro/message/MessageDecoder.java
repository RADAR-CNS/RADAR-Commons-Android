/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.avro.message;

import org.apache.avro.util.ByteBufferInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Deserializes a single datum from a ByteBuffer, byte array, or InputStream.
 * 
 * @param <D> a datum class
 */
public interface MessageDecoder<D> {

  /**
   * Deserialize a single datum from an InputStream.
   *
   * @param stream stream to read from
   * @param reuse  a datum instance to reuse, avoiding instantiation if possible
   * @return a datum read from the stream
   * @throws BadHeaderException     If the payload's header is not recognized.
   * @throws MissingSchemaException If the payload's schema cannot be found.
   * @throws IOException
   */
  D decode(InputStream stream, D reuse) throws IOException;

  /**
   * Deserialize a single datum from a ByteBuffer.
   *
   * @param encoded a ByteBuffer containing an encoded datum
   * @return a datum read from the stream
   * @throws BadHeaderException     If the payload's header is not recognized.
   * @throws MissingSchemaException If the payload's schema cannot be found.
   * @throws IOException
   */
  D decode(ByteBuffer encoded) throws IOException;

  /**
   * Base class for {@link MessageEncoder} implementations that provides default
   * implementations for most of the {@code DatumEncoder} API.
   * <p>
   * Implementations provided by this base class are thread-safe.
   *
   * @param <D> a datum class
   */
  abstract class BaseDecoder<D> implements MessageDecoder<D> {
    @Override
    public D decode(ByteBuffer encoded) throws IOException {
      return decode(new ByteBufferInputStream(encoded), null);
    }
  }
}
