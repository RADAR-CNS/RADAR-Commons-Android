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

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.io.parsing.Symbols;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * {@link Decoder} that performs type-resolution between the reader's and
 * writer's schemas.
 *
 * <p>
 * When resolving schemas, this class will return the values of fields in
 * _writer's_ order, not the reader's order. (However, it returns _only_ the
 * reader's fields, not any extra fields the writer may have written.) To help
 * clients handle fields that appear to be coming out of order, this class
 * defines the method {@link #readFieldOrder}.
 *
 * <p>
 * See the <a href="parsing/doc-files/parsing.html">parser documentation</a> for
 * information on how this works.
 */
public class ResolvingDecoder extends ValidatingDecoder {

  private Decoder backup;

  ResolvingDecoder(Schema writer, Schema reader, Decoder in) throws IOException {
    this(resolve(writer, reader), in);
  }

  /**
   * Constructs a <tt>ResolvingDecoder</tt> using the given resolver. The resolver
   * must have been returned by a previous call to
   * {@link #resolve(Schema, Schema)}.
   *
   * @param resolver The resolver to use.
   * @param in       The underlying decoder.
   * @throws IOException
   */
  private ResolvingDecoder(Object resolver, Decoder in) throws IOException {
    super((Symbol) resolver, in);
  }

  /**
   * Produces an opaque resolver that can be used to construct a new
   * {@link ResolvingDecoder#ResolvingDecoder(Object, Decoder)}. The returned
   * Object is immutable and hence can be simultaneously used in many
   * ResolvingDecoders. This method is reasonably expensive, the users are
   * encouraged to cache the result.
   *
   * @param writer The writer's schema. Cannot be null.
   * @param reader The reader's schema. Cannot be null.
   * @return The opaque resolver.
   * @throws IOException
   * @throws NullPointerException if {@code writer} or {@code reader} is
   *                              {@code null}
   */
  public static Object resolve(Schema writer, Schema reader) throws IOException {
    Objects.requireNonNull(writer, "Writer schema cannot be null");
    Objects.requireNonNull(reader, "Reader schema cannot be null");
    return new ResolvingGrammarGenerator().generate(writer, reader);
  }

  /**
   * Returns the actual order in which the reader's fields will be returned to the
   * reader.
   *
   * This method is useful because {@link ResolvingDecoder} returns values in the
   * order written by the writer, rather than the order expected by the reader.
   * This method allows readers to figure out what fields to expect. Let's say the
   * reader is expecting a three-field record, the first field is a long, the
   * second a string, and the third an array. In this case, a typical usage might
   * be as follows:
   *
   * <pre>
   *   Schema.Fields[] fieldOrder = in.readFieldOrder();
   *   for (int i = 0; i &lt; 3; i++) {
   *     switch (fieldOrder[i].pos()) {
   *     case 1:
   *       foo(in.readLong());
   *       break;
   *     case 2:
   *       someVariable = in.readString();
   *       break;
   *     case 3:
   *       bar(in); // The code of "bar" will read an array-of-int
   *       break;
   *     }
   * </pre>
   *
   * Note that {@link ResolvingDecoder} will return only the fields expected by
   * the reader, not other fields that may have been written by the writer. Thus,
   * the iteration-count of "3" in the above loop will always be correct.
   *
   * Throws a runtime exception if we're not just about to read the first field of
   * a record. (If the client knows the order of incoming fields, then the client
   * does <em>not</em> need to call this method but rather can just start reading
   * the field values.)
   *
   * @throws AvroTypeException If we're not starting a new record
   *
   */
  public final Schema.Field[] readFieldOrder() throws IOException {
    return ((Symbol.FieldOrderAction) parser.advance(Symbols.FIELD_ACTION)).fields;
  }

  /**
   * Consume any more data that has been written by the writer but not needed by
   * the reader so that the the underlying decoder is in proper shape for the next
   * record. This situation happens when, for example, the writer writes a record
   * with two fields and the reader needs only the first field.
   *
   * This function should be called after completely decoding an object but before
   * next object can be decoded from the same underlying decoder either directly
   * or through another resolving decoder. If the same resolving decoder is used
   * for the next object as well, calling this method is optional; the state of
   * this resolving decoder ensures that any leftover portions are consumed before
   * the next object is decoded.
   *
   * @throws IOException
   */
  public final void drain() throws IOException {
    parser.processImplicitActions();
  }

  @Override
  public long readLong() throws IOException {
    Symbol actual = parser.advance(Symbols.LONG);
    if (actual == Symbols.INT) {
      return in.readInt();
    } else if (actual == Symbols.DOUBLE) {
      return (long) in.readDouble();
    } else {
      assert actual == Symbols.LONG;
      return in.readLong();
    }
  }

  @Override
  public float readFloat() throws IOException {
    Symbol actual = parser.advance(Symbols.FLOAT);
    if (actual == Symbols.INT) {
      return (float) in.readInt();
    } else if (actual == Symbols.LONG) {
      return (float) in.readLong();
    } else {
      assert actual == Symbols.FLOAT;
      return in.readFloat();
    }
  }

  @Override
  public double readDouble() throws IOException {
    Symbol actual = parser.advance(Symbols.DOUBLE);
    if (actual == Symbols.INT) {
      return (double) in.readInt();
    } else if (actual == Symbols.LONG) {
      return (double) in.readLong();
    } else if (actual == Symbols.FLOAT) {
      return (double) in.readFloat();
    } else {
      assert actual == Symbols.DOUBLE;
      return in.readDouble();
    }
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    Symbol actual = parser.advance(Symbols.STRING);
    if (actual == Symbols.BYTES) {
      return new Utf8(in.readBytes(null).array());
    } else {
      assert actual == Symbols.STRING;
      return in.readString(old);
    }
  }

  @Override
  public String readString() throws IOException {
    Symbol actual = parser.advance(Symbols.STRING);
    if (actual == Symbols.BYTES) {
      return new String(in.readBytes(null).array(), StandardCharsets.UTF_8);
    } else {
      assert actual == Symbols.STRING;
      return in.readString();
    }
  }

  @Override
  public void skipString() throws IOException {
    Symbol actual = parser.advance(Symbols.STRING);
    if (actual == Symbols.BYTES) {
      in.skipBytes();
    } else {
      assert actual == Symbols.STRING;
      in.skipString();
    }
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    Symbol actual = parser.advance(Symbols.BYTES);
    if (actual == Symbols.STRING) {
      Utf8 s = in.readString(null);
      return ByteBuffer.wrap(s.getBytes(), 0, s.getByteLength());
    } else {
      assert actual == Symbols.BYTES;
      return in.readBytes(old);
    }
  }

  @Override
  public void skipBytes() throws IOException {
    Symbol actual = parser.advance(Symbols.BYTES);
    if (actual == Symbols.STRING) {
      in.skipString();
    } else {
      assert actual == Symbols.BYTES;
      in.skipBytes();
    }
  }

  @Override
  public int readEnum() throws IOException {
    parser.advance(Symbols.ENUM);
    Symbol.EnumAdjustAction top = (Symbol.EnumAdjustAction) parser.popSymbol();
    int n = in.readEnum();
    if (top.noAdjustments) {
      return n;
    }
    Object o = top.adjustments[n];
    if (o instanceof Integer) {
      return (Integer) o;
    } else {
      throw new AvroTypeException((String) o);
    }
  }

  @Override
  public int readIndex() throws IOException {
    parser.advance(Symbols.UNION);
    Symbol top = parser.popSymbol();
    final int result;
    if (top instanceof Symbol.UnionAdjustAction) {
      result = ((Symbol.UnionAdjustAction) top).rindex;
      top = ((Symbol.UnionAdjustAction) top).symToParse;
    } else {
      result = in.readIndex();
      top = ((Symbol.Alternative) top).getSymbol(result);
    }
    parser.pushSymbol(top);
    return result;
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top instanceof Symbol.FieldOrderAction) {
      return input == Symbols.FIELD_ACTION ? top : null;
    }
    if (top instanceof Symbol.ResolvingAction) {
      Symbol.ResolvingAction t = (Symbol.ResolvingAction) top;
      if (t.reader != input) {
        throw new AvroTypeException("Found " + t.reader + " while looking for " + input);
      } else {
        return t.writer;
      }
    } else if (top instanceof Symbol.SkipAction) {
      Symbol symToSkip = ((Symbol.SkipAction) top).symToSkip;
      parser.skipSymbol(symToSkip);
    } else if (top instanceof Symbol.WriterUnionAction) {
      Symbol.Alternative branches = (Symbol.Alternative) parser.popSymbol();
      parser.pushSymbol(branches.getSymbol(in.readIndex()));
    } else if (top instanceof Symbol.ErrorAction) {
      throw new AvroTypeException(((Symbol.ErrorAction) top).msg);
    } else if (top instanceof Symbol.DefaultStartAction) {
      Symbol.DefaultStartAction dsa = (Symbol.DefaultStartAction) top;
      backup = in;
      in = DecoderFactory.get().binaryDecoder(dsa.contents, null);
    } else if (top == Symbols.DEFAULT_END_ACTION) {
      in = backup;
    } else {
      throw new AvroTypeException("Unknown action: " + top);
    }
    return null;
  }

  @Override
  public void skipAction() throws IOException {
    Symbol top = parser.popSymbol();
    if (top instanceof Symbol.ResolvingAction) {
      parser.pushSymbol(((Symbol.ResolvingAction) top).writer);
    } else if (top instanceof Symbol.SkipAction) {
      parser.pushSymbol(((Symbol.SkipAction) top).symToSkip);
    } else if (top instanceof Symbol.WriterUnionAction) {
      Symbol.Alternative branches = (Symbol.Alternative) parser.popSymbol();
      parser.pushSymbol(branches.getSymbol(in.readIndex()));
    } else if (top instanceof Symbol.ErrorAction) {
      throw new AvroTypeException(((Symbol.ErrorAction) top).msg);
    } else if (top instanceof Symbol.DefaultStartAction) {
      Symbol.DefaultStartAction dsa = (Symbol.DefaultStartAction) top;
      backup = in;
      in = DecoderFactory.get().binaryDecoder(dsa.contents, null);
    } else if (top == Symbols.DEFAULT_END_ACTION) {
      in = backup;
    }
  }
}
