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
package org.apache.avro.generic;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/** {@link DatumReader} for generic Java objects. */
public class GenericDatumReader<D> implements DatumReader<D> {
  private final GenericData data;
  private Schema actual;
  private Schema expected;

  private ResolvingDecoder creatorResolver = null;
  private final Thread creator;

  /** Construct where the writer's and reader's schemas are the same. */
  public GenericDatumReader(Schema schema) {
    this(schema, schema, GenericData.get());
  }

  /** Construct given writer's and reader's schema. */
  public GenericDatumReader(Schema writer, Schema reader) {
    this(writer, reader, GenericData.get());
  }

  public GenericDatumReader(Schema writer, Schema reader, GenericData data) {
    this(data);
    this.actual = writer;
    this.expected = reader;
  }

  protected GenericDatumReader(GenericData data) {
    this.data = data;
    this.creator = Thread.currentThread();
  }

  /** Return the {@link GenericData} implementation. */
  public GenericData getData() {
    return data;
  }

  /** Return the writer's schema. */
  public Schema getSchema() {
    return actual;
  }

  /** Get the reader's schema. */
  public Schema getExpected() {
    return expected;
  }

  /** Set the reader's schema. */
  public void setExpected(Schema reader) {
    this.expected = reader;
    creatorResolver = null;
  }

  /**
   * Gets a resolving decoder for use by this GenericDatumReader. Unstable API.
   * Currently uses a thread local cache to prevent constructing the resolvers too
   * often, because that is very expensive.
   */
  protected final ResolvingDecoder getResolver(Schema actual, Schema expected) throws IOException {
    Thread currThread = Thread.currentThread();
    ResolvingDecoder resolver;
    if (currThread == creator && creatorResolver != null) {
      return creatorResolver;
    }

    resolver = DecoderFactory.get()
            .resolvingDecoder(Schema.applyAliases(actual, expected), expected, null);

    if (currThread == creator) {
      creatorResolver = resolver;
    }

    return resolver;
  }

  @Override
  @SuppressWarnings("unchecked")
  public D read(D reuse, Decoder in) throws IOException {
    ResolvingDecoder resolver = getResolver(actual, expected);
    resolver.configure(in);
    D result = (D) read(reuse, expected, resolver);
    resolver.drain();
    return result;
  }

  /** Called to read data. */
  protected Object read(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    return readWithoutConversion(old, expected, in);
  }

  protected Object readWithoutConversion(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    switch (expected.getType()) {
    case RECORD:
      return readRecord(old, expected, in);
    case ENUM:
      return readEnum(expected, in);
    case ARRAY:
      return readArray(old, expected, in);
    case MAP:
      return readMap(old, expected, in);
    case UNION:
      return read(old, expected.getTypes().get(in.readIndex()), in);
    case FIXED:
      return readFixed(old, expected, in);
    case STRING:
      return readString(in);
    case BYTES:
      return readBytes(old, in);
    case INT:
      return readInt(in);
    case LONG:
      return in.readLong();
    case FLOAT:
      return in.readFloat();
    case DOUBLE:
      return in.readDouble();
    case BOOLEAN:
      return in.readBoolean();
    case NULL:
      in.readNull();
      return null;
    default:
      throw new AvroRuntimeException("Unknown type: " + expected);
    }
  }

  /**
   * Called to read a record instance. May be overridden for alternate record
   * representations.
   */
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    final Object record = data.newRecord(old, expected);

    for (Field field : in.readFieldOrder()) {
      int pos = field.pos();
      String name = field.name();
      Object oldDatum = null;
      if (old != null) {
        oldDatum = data.getField(record, name, pos);
      }

      readField(record, field, oldDatum, in);
    }

    return record;
  }

  /**
   * Called to read a single field of a record. May be overridden for more
   * efficient or alternate implementations.
   */
  protected void readField(Object record, Field field, Object oldDatum, ResolvingDecoder in)
      throws IOException {
    data.setField(record, field.name(), field.pos(), read(oldDatum, field.schema(), in));
  }

  /**
   * Called to read an enum value. May be overridden for alternate enum
   * representations. By default, returns a GenericEnumSymbol.
   */
  protected Object readEnum(Schema expected, Decoder in) throws IOException {
    return createEnum(expected.getEnumSymbols().get(in.readEnum()), expected);
  }

  /**
   * Called to create an enum value. May be overridden for alternate enum
   * representations. By default, returns a GenericEnumSymbol.
   */
  protected Object createEnum(String symbol, Schema schema) {
    return data.createEnum(symbol, schema);
  }

  /**
   * Called to read an array instance. May be overridden for alternate array
   * representations.
   */
  protected Object readArray(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    Schema expectedType = expected.getElementType();
    long l = in.readArrayStart();
    long base = 0;
    if (l > 0) {
      Object array = newArray(old, (int) l, expected);
      do {
        for (long i = 0; i < l; i++) {
          addToArray(array, base + i, readWithoutConversion(peekArray(array), expectedType, in));
        }
        base += l;
      } while ((l = in.arrayNext()) > 0);
      return pruneArray(array);
    } else {
      return pruneArray(newArray(old, 0, expected));
    }
  }

  private Object pruneArray(Object object) {
    if (object instanceof GenericArray<?>) {
      ((GenericArray<?>) object).prune();
    }
    return object;
  }

  /**
   * Called by the default implementation of {@link #readArray} to retrieve a
   * value from a reused instance. The default implementation is for
   * {@link GenericArray}.
   */
  @SuppressWarnings("unchecked")
  protected Object peekArray(Object array) {
    return (array instanceof GenericArray) ? ((GenericArray) array).peek() : null;
  }

  /**
   * Called by the default implementation of {@link #readArray} to add a value.
   * The default implementation is for {@link Collection}.
   */
  @SuppressWarnings("unchecked")
  protected void addToArray(Object array, long pos, Object e) {
    ((Collection) array).add(e);
  }

  /**
   * Called to read a map instance. May be overridden for alternate map
   * representations.
   */
  protected Object readMap(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    Schema eValue = expected.getValueType();
    long l = in.readMapStart();
    Object map = newMap(old, (int) l);
    if (l > 0) {
      do {
        for (int i = 0; i < l; i++) {
          addToMap(map, readMapKey(in), readWithoutConversion(null, eValue, in));
        }
      } while ((l = in.mapNext()) > 0);
    }
    return map;
  }

  /**
   * Called by the default implementation of {@link #readMap} to read a key value.
   * The default implementation returns delegates to
   * {@link #readString(Object, org.apache.avro.io.Decoder)}.
   */
  protected Object readMapKey(Decoder in) throws IOException {
    return readString(in);
  }

  /**
   * Called by the default implementation of {@link #readMap} to add a key/value
   * pair. The default implementation is for {@link Map}.
   */
  @SuppressWarnings("unchecked")
  protected void addToMap(Object map, Object key, Object value) {
    ((Map) map).put(key, value);
  }

  /**
   * Called to read a fixed value. May be overridden for alternate fixed
   * representations. By default, returns {@link GenericFixed}.
   */
  protected Object readFixed(Object old, Schema expected, Decoder in) throws IOException {
    GenericFixed fixed = (GenericFixed) data.createFixed(old, expected);
    in.readFixed(fixed.bytes(), 0, expected.getFixedSize());
    return fixed;
  }

  /**
   * Called to create an fixed value. May be overridden for alternate fixed
   * representations. By default, returns {@link GenericFixed}.
   *
   * @deprecated As of Avro 1.6.0 this method has been moved to
   *             {@link GenericData#createFixed(Object, Schema)}
   */
  @Deprecated
  protected Object createFixed(Object old, Schema schema) {
    return data.createFixed(old, schema);
  }

  /**
   * Called to create an fixed value. May be overridden for alternate fixed
   * representations. By default, returns {@link GenericFixed}.
   *
   * @deprecated As of Avro 1.6.0 this method has been moved to
   *             {@link GenericData#createFixed(Object, byte[], Schema)}
   */
  @Deprecated
  protected Object createFixed(Object old, byte[] bytes, Schema schema) {
    return data.createFixed(old, bytes, schema);
  }

  /**
   * Called to create new record instances. Subclasses may override to use a
   * different record implementation. The returned instance must conform to the
   * schema provided. If the old object contains fields not present in the schema,
   * they should either be removed from the old object, or it should create a new
   * instance that conforms to the schema. By default, this returns a
   * {@link GenericData.Record}.
   *
   * @deprecated As of Avro 1.6.0 this method has been moved to
   *             {@link GenericData#newRecord(Object, Schema)}
   */
  @Deprecated
  protected Object newRecord(Object old, Schema schema) {
    return data.newRecord(old, schema);
  }

  /**
   * Called to create new array instances. Subclasses may override to use a
   * different array implementation. By default, this returns a
   * {@link GenericData.Array}.
   */
  @SuppressWarnings("unchecked")
  protected Object newArray(Object old, int size, Schema schema) {
    return data.newArray(old, size, schema);
  }

  /**
   * Called to create new array instances. Subclasses may override to use a
   * different map implementation. By default, this returns a {@link HashMap}.
   */
  @SuppressWarnings("unchecked")
  protected Object newMap(Object old, int size) {
    return data.newMap(old, size);
  }

  /**
   * Called to read strings. Subclasses may override to use a different string
   * representation. By default, this calls {@link Decoder#readString()}.
   */
  protected Object readString(Decoder in) throws IOException {
    return in.readString();
  }

  /**
   * Called to read byte arrays. Subclasses may override to use a different byte
   * array representation. By default, this calls
   * {@link Decoder#readBytes(ByteBuffer)}.
   */
  protected Object readBytes(Object old, Decoder in) throws IOException {
    return in.readBytes(old instanceof ByteBuffer ? (ByteBuffer) old : null);
  }

  /**
   * Called to read integers. Subclasses may override to use a different integer
   * representation. By default, this calls {@link Decoder#readInt()}.
   */
  protected Object readInt(Decoder in) throws IOException {
    return in.readInt();
  }

  /**
   * Called to create byte arrays from default values. Subclasses may override to
   * use a different byte array representation. By default, this calls
   * {@link ByteBuffer#wrap(byte[])}.
   */
  protected Object createBytes(byte[] value) {
    return ByteBuffer.wrap(value);
  }
}
