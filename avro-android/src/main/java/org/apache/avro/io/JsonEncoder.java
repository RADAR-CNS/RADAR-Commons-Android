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
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.io.parsing.Symbols;
import org.apache.avro.util.Utf8;
import org.json.JSONException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

/**
 * An {@link Encoder} for Avro's JSON data encoding.
 * </p>
 * Construct using {@link EncoderFactory}.
 * </p>
 * JsonEncoder buffers output, and data may not appear on the output until
 * {@link Encoder#flush()} is called.
 * </p>
 * JsonEncoder is not thread-safe.
 */
public class JsonEncoder extends ParsingEncoder implements Parser.ActionHandler {
  private final Parser parser;
  private final JSONWriter out;
  private final Writer rawWriter;

  /**
   * Has anything been written into the collections?
   */
  protected final BitSet isEmpty = new BitSet();

  JsonEncoder(Schema sc, OutputStream out) {
    rawWriter = new OutputStreamWriter(out);
    this.out = new JSONWriter(rawWriter);
    this.parser = new Parser(new JsonGrammarGenerator().generate(sc), this);
  }

  @Override
  public void flush() throws IOException {
    parser.processImplicitActions();
    rawWriter.flush();
  }

  @Override
  public void writeNull() throws IOException {
    parser.advance(Symbols.NULL);
    try {
      out.value(null);
    } catch (JSONException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    parser.advance(Symbols.BOOLEAN);
    try {
      out.value(b);
    } catch (JSONException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void writeInt(int n) throws IOException {
    parser.advance(Symbols.INT);
    try {
      out.value(n);
    } catch (JSONException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void writeLong(long n) throws IOException {
    parser.advance(Symbols.LONG);
    try {
      out.value(n);
    } catch (JSONException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void writeFloat(float f) throws IOException {
    parser.advance(Symbols.FLOAT);
    try {
      out.value(f);
    } catch (JSONException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void writeDouble(double d) throws IOException {
    parser.advance(Symbols.DOUBLE);
    try {
      out.value(d);
    } catch (JSONException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    writeString(utf8.toString());
  }

  @Override
  public void writeString(String str) throws IOException {
    parser.advance(Symbols.STRING);
    try {
      if (parser.topSymbol() == Symbols.MAP_KEY_MARKER) {
        parser.advance(Symbols.MAP_KEY_MARKER);
        out.key(str);
      } else {
        out.value(str);
      }
    } catch (JSONException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    if (bytes.hasArray()) {
      writeBytes(bytes.array(), bytes.position(), bytes.remaining());
    } else {
      byte[] b = new byte[bytes.remaining()];
      bytes.duplicate().get(b);
      writeBytes(b);
    }
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbols.BYTES);
    writeByteArray(bytes, start, len);
  }

  private void writeByteArray(byte[] bytes, int start, int len) throws IOException {
    try {
      out.value(new String(bytes, start, len, StandardCharsets.ISO_8859_1));
    } catch (JSONException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbols.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    if (len != top.size) {
      throw new AvroTypeException(
          "Incorrect length for fixed binary: expected " + top.size + " but received " + len + " bytes.");
    }
    writeByteArray(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    parser.advance(Symbols.ENUM);
    Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction) parser.popSymbol();
    if (e < 0 || e >= top.size) {
      throw new AvroTypeException("Enumeration out of range: max is " + top.size + " but received " + e);
    }
    try {
      out.value(top.getLabel(e));
    } catch (JSONException jsonException) {
      throw new IOException(jsonException);
    }
  }

  @Override
  public void writeArrayStart() throws IOException {
    parser.advance(Symbols.ARRAY_START);
    try {
      out.array();
    } catch (JSONException e) {
      throw new IOException(e);
    }
    push();
    isEmpty.set(depth());
  }

  @Override
  public void writeArrayEnd() throws IOException {
    if (!isEmpty.get(pos)) {
      parser.advance(Symbols.ITEM_END);
    }
    pop();
    parser.advance(Symbols.ARRAY_END);
    try {
      out.endArray();
    } catch (JSONException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void writeMapStart() throws IOException {
    push();
    isEmpty.set(depth());

    parser.advance(Symbols.MAP_START);
    try {
      out.object();
    } catch (JSONException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void writeMapEnd() throws IOException {
    if (!isEmpty.get(pos)) {
      parser.advance(Symbols.ITEM_END);
    }
    pop();

    parser.advance(Symbols.MAP_END);
    try {
      out.endObject();
    } catch (JSONException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void startItem() throws IOException {
    if (!isEmpty.get(pos)) {
      parser.advance(Symbols.ITEM_END);
    }
    super.startItem();
    isEmpty.clear(depth());
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    parser.advance(Symbols.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    Symbol symbol = top.getSymbol(unionIndex);
    if (symbol != Symbols.NULL) {
      try {
        out.object();
        out.key(top.getLabel(unionIndex));
      } catch (JSONException e) {
        throw new IOException(e);
      }
      parser.pushSymbol(Symbols.UNION_END);
    }
    parser.pushSymbol(symbol);
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) {
    try {
      if (top instanceof Symbol.FieldAdjustAction) {
        Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
        out.key(fa.fname);
      } else if (top == Symbols.RECORD_START) {
        out.object();
      } else if (top == Symbols.RECORD_END || top == Symbols.UNION_END) {
        out.endObject();
      } else if (top != Symbols.FIELD_END) {
        throw new AvroTypeException("Unknown action symbol " + top);
      }
    } catch (JSONException | IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
