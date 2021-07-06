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
import org.apache.avro.util.Utf8;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.apache.avro.io.parsing.Symbol.ARRAY_END;
import static org.apache.avro.io.parsing.Symbol.MAP_END;
import static org.apache.avro.io.parsing.Symbol.MAP_START;
import static org.apache.avro.io.parsing.Symbol.RECORD_END;
import static org.apache.avro.io.parsing.Symbol.RECORD_START;

/**
 * A {@link Decoder} for Avro's JSON data encoding.
 * </p>
 * Construct using {@link DecoderFactory}.
 * </p>
 * JsonDecoder is not thread-safe.
 */
public class JsonDecoder extends ParsingDecoder implements Parser.ActionHandler {
  private final Deque<JSONStackElement> stateDeque;
  private JSONStackElement currentState;

  private JsonDecoder(Symbol root, InputStream in) throws IOException {
    super(root);
    InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
    StringBuilder builder = new StringBuilder();
    char[] buffer = new char[8192];
    int numRead = reader.read(buffer);
    while (numRead != -1) {
      builder.append(buffer, 0, numRead);
      numRead = reader.read(buffer);
    }
    JSONTokener tokener = new JSONTokener(builder.toString());
    stateDeque = new ArrayDeque<>();
    try {
      currentState = new JSONPrimitiveStackElement(tokener.nextValue());
    } catch (JSONException ex) {
      throw new IOException(ex);
    }
  }

  private void advance(Symbol symbol) throws IOException {
    this.parser.processTrailingImplicitActions();
    parser.advance(symbol);
  }

  JsonDecoder(Schema schema, InputStream in) throws IOException {
    this(getSymbol(schema), in);
  }

  private static Symbol getSymbol(Schema schema) {
    Objects.requireNonNull(schema, "Schema cannot be null");
    return new JsonGrammarGenerator().generate(schema);
  }

  @Override
  public void readNull() throws IOException {
    advance(Symbol.NULL);
    Object result = currentState.next("null");
    if (result != JSONObject.NULL) {
      throw error("null", result);
    }
  }

  @Override
  public boolean readBoolean() throws IOException {
    advance(Symbol.BOOLEAN);
    Object result = currentState.next("boolean");
    if (result instanceof Boolean) {
      return (boolean) result;
    } else {
      throw error("boolean", result);
    }
  }

  @Override
  public int readInt() throws IOException {
    advance(Symbol.INT);
    Object result = currentState.next("int");
    if (result instanceof Number) {
      return ((Number) result).intValue();
    } else {
      throw error("int", result);
    }
  }

  @Override
  public long readLong() throws IOException {
    advance(Symbol.LONG);
    Object result = currentState.next("long");
    if (result instanceof Number) {
      return ((Number) result).longValue();
    } else {
      throw error("long", result);
    }
  }

  @Override
  public float readFloat() throws IOException {
    advance(Symbol.FLOAT);
    Object result = currentState.next("float");
    if (result instanceof Number) {
      return ((Number) result).floatValue();
    } else {
      throw error("float", result);
    }
  }

  @Override
  public double readDouble() throws IOException {
    advance(Symbol.DOUBLE);
    Object result = currentState.next("double");
    if (result instanceof Number) {
      return ((Number) result).doubleValue();
    } else {
      throw error("double", result);
    }
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    if (old != null) {
      old.set(readString());
    }
    return new Utf8(readString());
  }

  @Override
  public String readString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      return readString("map-key");
    } else {
      return readString("string");
    }
  }

  private String readString(String type) throws IOException {
    Object result = currentState.next(type);
    if (!(result instanceof String)) {
      throw error(type, result);
    }
    return (String) result;
  }

  @Override
  public void skipString() throws IOException {
    advance(Symbol.STRING);
    readString();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    advance(Symbol.BYTES);
    byte[] result = readByteArray("bytes");
    return ByteBuffer.wrap(result);
  }

  private byte[] readByteArray(String type) throws IOException {
    return readString(type).getBytes(StandardCharsets.ISO_8859_1);
  }

  @Override
  public void skipBytes() throws IOException {
    advance(Symbol.BYTES);
    readByteArray("bytes");
  }

  private void checkFixed(int size) {
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    if (size != top.size) {
      throw new AvroTypeException(
          "Incorrect length for fixed binary: expected " + top.size + " but received " + size + " bytes.");
    }
  }

  @Override
  public void readFixed(byte[] bytes, int start, int len) throws IOException {
    advance(Symbol.FIXED);
    checkFixed(len);
    byte[] result = readByteArray("fixed");
    if (result.length != len) {
      throw new AvroTypeException("Expected fixed length " + len + ", but got" + result.length);
    }
    System.arraycopy(result, 0, bytes, start, len);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    advance(Symbol.FIXED);
    checkFixed(length);
    doSkipFixed(length);
  }

  private void doSkipFixed(int length) throws IOException {
    checkFixed(length);
    byte[] result = readByteArray("fixed");
    if (result.length != length) {
      throw new AvroTypeException("Expected fixed length " + length + ", but got" + result.length);
    }
  }

  @Override
  protected void skipFixed() throws IOException {
    advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    doSkipFixed(top.size);
  }

  @Override
  public int readEnum() throws IOException {
    advance(Symbol.ENUM);
    Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction) parser.popSymbol();
    String symbol = readString("enum");
    int n = top.findLabel(symbol);
    if (n >= 0) {
      return n;
    }
    throw new AvroTypeException("Unknown symbol in enum " + symbol);
  }

  @Override
  public long readArrayStart() throws IOException {
    advance(Symbol.ARRAY_START);
    Object next = currentState.next("array-start");
    if (next instanceof JSONArray) {
      stateDeque.push(currentState);
      currentState = new JSONArrayStackElement((JSONArray) next);
    }
    return doArrayNext();
  }

  @Override
  public long arrayNext() throws IOException {
    return doArrayNext();
  }

  private long doArrayNext() throws IOException {
    if (currentState instanceof JSONArrayStackElement) {
      int result = currentState.available();
      if (result == 0) {
        advance(ARRAY_END);
        currentState = stateDeque.pop();
      }
      return result;
    } else {
      throw error("array", currentState.getClass().getSimpleName());
    }
  }

  @Override
  public long skipArray() throws IOException {
    advance(Symbol.ARRAY_START);
    Object next = currentState.next("array-start");
    if (!(next instanceof JSONArray)) {
      throw error("array-start", next);
    }
    advance(ARRAY_END);
    currentState = stateDeque.pop();
    return 0;
  }

  @Override
  public long readMapStart() throws IOException {
    if (parser.topSymbol() == RECORD_START) {
      advance(Symbol.RECORD_START);
      Object next = currentState.next("record-start");
      if (next instanceof JSONObject) {
        stateDeque.push(currentState);
        currentState = new JSONRecordStackElement((JSONObject) next);
      }
    } else if (parser.topSymbol() == MAP_START) {
      advance(Symbol.MAP_START);
      Object next = currentState.next("map-start");
      if (next instanceof JSONObject) {
        stateDeque.push(currentState);
        currentState = new JSONMapStackElement((JSONObject) next);
      }
    }
    return doMapNext();
  }

  @Override
  public long mapNext() throws IOException {
    return doMapNext();
  }

  private long doMapNext() throws IOException {
    if (currentState instanceof JSONRecordStackElement
            || currentState instanceof JSONMapStackElement) {
      int result = currentState.available();
      if (result == 0) {
        if (currentState instanceof JSONRecordStackElement) {
          advance(RECORD_END);
        } else {
          advance(MAP_END);
        }
        currentState = stateDeque.pop();
      }
      return result;
    } else {
      throw error("array", currentState.getClass().getSimpleName());
    }
  }

  @Override
  public long skipMap() throws IOException {
    advance(Symbol.MAP_START);
    Object next = currentState.next("map-start");
    if (!(next instanceof JSONObject)) {
      throw error("map-start", next);
    }
    advance(MAP_END);
    currentState = stateDeque.pop();
    return 0;
  }

  @Override
  public int readIndex() throws IOException {
    advance(Symbol.UNION);
    Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

    String label;
    Object next = currentState.next("start-union");
    if (next == JSONObject.NULL) {
      label = "null";
    } else if (next instanceof JSONObject) {
      try {
        label = ((JSONObject) next).keys().next();
      } catch (NoSuchElementException ex) {
        throw error("start-union", "empty-object");
      }
      stateDeque.push(currentState);
      try {
        currentState = new JSONPrimitiveStackElement(((JSONObject) next).get(label));
      } catch (JSONException ex) {
        throw new IOException(ex);
      }
      parser.pushSymbol(Symbol.UNION_END);
    } else {
      throw error("start-union", next);
    }
    int n = a.findLabel(label);
    if (n < 0)
      throw new AvroTypeException("Unknown union branch " + label);
    parser.pushSymbol(a.getSymbol(n));
    return n;
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    return null;
  }

  private AvroTypeException error(String type, Object result) {
    return new AvroTypeException("Expected " + type + ". Got " + result);
  }

  private interface JSONStackElement {
    int available() throws IOException;
    Object next(String type) throws IOException;
  }

  private class JSONMapStackElement implements JSONStackElement {
    final JSONObject element;
    private final Iterator<String> keyIterator;
    String key;
    int queried;
    final int totalSize;

    JSONMapStackElement(JSONObject object) {
      element = object;
      keyIterator = element.keys();
      queried = 0;
      totalSize = element.length() * 2;
    }

    @Override
    public int available() {
      return totalSize - queried;
    }

    @Override
    public Object next(String type) throws IOException {
      if (key == null) {
        advance(Symbol.MAP_KEY_MARKER);
        if (!keyIterator.hasNext()) {
          throw new AvroTypeException("Expected " + type + ". Got map end.");
        }
        if (!type.equals("map-key")) {
          throw new AvroTypeException("Expected " + type + ". Got map-key string.");
        }
        queried++;
        key = keyIterator.next();
        return key;
      } else {
        advance(Symbol.ITEM_END);
        try {
          Object result = element.get(key);
          queried++;
          key = null;
          return result;
        } catch (JSONException ex) {
          throw new IOException(ex);
        }
      }
    }
  }

  private class JSONRecordStackElement implements JSONStackElement {
    final JSONObject element;
    private final Iterator<String> keyIterator;
    int queried;
    final int totalSize;

    JSONRecordStackElement(JSONObject object) {
      element = object;
      keyIterator = element.keys();
      queried = 0;
      totalSize = element.length();
    }

    @Override
    public int available() {
      return totalSize - queried;
    }

    @Override
    public Object next(String type) throws IOException {
      if (!keyIterator.hasNext()) {
        throw new AvroTypeException("Expected " + type + ". Got object end.");
      }
      advance(Symbol.ITEM_END);
      try {
        queried++;
        return element.get(keyIterator.next());
      } catch (JSONException ex) {
        throw new IOException(ex);
      }
    }
  }
  private class JSONArrayStackElement implements JSONStackElement {
    final JSONArray element;
    private int arrayIndex;
    final int totalSize;

    JSONArrayStackElement(JSONArray array) {
      element = array;
      arrayIndex = 0;
      totalSize = element.length();
    }

    @Override
    public int available() {
      return totalSize - arrayIndex;
    }

    @Override
    public Object next(String type) throws IOException {
      advance(Symbol.ITEM_END);
      if (arrayIndex >= element.length()) {
        throw new AvroTypeException("Expected " + type + ". Got array end.");
      }
      try {
        return element.get(arrayIndex++);
      } catch (JSONException ex) {
        throw new IOException(ex);
      }
    }
  }
  private static class JSONPrimitiveStackElement implements JSONStackElement {
    final Object element;
    boolean isRead;
    JSONPrimitiveStackElement(Object value) {
      element = value;
      isRead = false;
    }

    @Override
    public int available() {
      return isRead ? 0 : 1;
    }

    @Override
    public Object next(String type) {
      if (isRead) {
        throw new AvroTypeException("Expected " + type + ". Got data end.");
      }
      isRead = true;
      return element;
    }
  }
}
