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
package org.apache.avro;

import org.apache.avro.util.IdentityReference;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An abstract data type.
 * <p>
 * A schema may be one of:
 * <ul>
 * <li>A <i>record</i>, mapping field names to field value data;
 * <li>An <i>enum</i>, containing one of a small set of symbols;
 * <li>An <i>array</i> of values, all of the same schema;
 * <li>A <i>map</i>, containing string/value pairs, of a declared schema;
 * <li>A <i>union</i> of other schemas;
 * <li>A <i>fixed</i> sized binary object;
 * <li>A unicode <i>string</i>;
 * <li>A sequence of <i>bytes</i>;
 * <li>A 32-bit signed <i>int</i>;
 * <li>A 64-bit signed <i>long</i>;
 * <li>A 32-bit IEEE single-<i>float</i>; or
 * <li>A 64-bit IEEE <i>double</i>-float; or
 * <li>A <i>boolean</i>; or
 * <li><i>null</i>.
 * </ul>
 *
 * <li>{@link #setFields(List)}, can be called at most once. This method exists
 * in order to enable clients to build recursive schemas.
 * <li>It is not possible to change or delete an existing
 * property.
 * </ul>
 */
public abstract class Schema extends JsonProperties implements Serializable {

  private static final long serialVersionUID = 1L;

  protected Object writeReplace() {
    SerializableSchema ss = new SerializableSchema();
    ss.schemaString = toString();
    return ss;
  }

  private static final class SerializableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private String schemaString;

    private Object readResolve() {
      return new Schema.Parser().parse(schemaString);
    }
  }

  private static final int NO_HASHCODE = Integer.MIN_VALUE;

  /** The type of a schema. */
  public enum Type {
    RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL;

    private final String name;

    Type() {
      this.name = this.name().toLowerCase(Locale.ENGLISH);
    }

    public String getName() {
      return name;
    }
  }

  private final Type type;

  Schema(Type type) {
    super();
    this.type = type;
  }

  /** Create a schema for a primitive type. */
  public static Schema create(Type type) {
    switch (type) {
    case STRING:
      return new StringSchema();
    case BYTES:
      return new BytesSchema();
    case INT:
      return new IntSchema();
    case LONG:
      return new LongSchema();
    case FLOAT:
      return new FloatSchema();
    case DOUBLE:
      return new DoubleSchema();
    case BOOLEAN:
      return new BooleanSchema();
    case NULL:
      return new NullSchema();
    default:
      throw new AvroRuntimeException("Can't create a: " + type);
    }
  }

  private static final Set<String> SCHEMA_RESERVED = new HashSet<>(
      Arrays.asList("doc", "fields", "items", "name", "namespace", "size", "symbols", "values", "type", "aliases"));

  private static final Set<String> ENUM_RESERVED = new HashSet<>(SCHEMA_RESERVED);
  static {
    ENUM_RESERVED.add("default");
  }

  int hashCode = NO_HASHCODE;

  @Override
  public void addProp(String name, Object value) {
    super.addProp(name, value);
    hashCode = NO_HASHCODE;
  }

  /** Create a named record schema. */
  public static Schema createRecord(String name, String doc, String namespace, boolean isError) {
    return new RecordSchema(new Name(name, namespace), doc, isError);
  }

  /** Create an enum schema. */
  public static Schema createEnum(String name, String doc, String namespace, List<String> values, String enumDefault) {
    return new EnumSchema(new Name(name, namespace), doc, Collections.unmodifiableList(new ArrayList<>(values)), enumDefault);
  }

  /** Create an array schema. */
  public static Schema createArray(Schema elementType) {
    return new ArraySchema(elementType);
  }

  /** Create a map schema. */
  public static Schema createMap(Schema valueType) {
    return new MapSchema(valueType);
  }

  /** Create a union schema. */
  public static Schema createUnion(List<Schema> types) {
    return new UnionSchema(types);
  }

  /** Create a fixed schema. */
  public static Schema createFixed(String name, String doc, String space, int size) {
    return new FixedSchema(new Name(name, space), doc, size);
  }

  /** Return the type of this schema. */
  public Type getType() {
    return type;
  }

  /**
   * If this is a record, returns the Field with the given name
   * <tt>fieldName</tt>. If there is no field by that name, a <tt>null</tt> is
   * returned.
   */
  public Field getField(String fieldname) {
    throw new AvroRuntimeException("Not a record: " + this);
  }

  /**
   * If this is a record, returns the fields in it. The returned list is in the
   * order of their positions.
   */
  public List<Field> getFields() {
    throw new AvroRuntimeException("Not a record: " + this);
  }

  /**
   * If this is a record, set its fields. The fields can be set only once in a
   * schema.
   */
  public void setFields(List<Field> fields) {
    throw new AvroRuntimeException("Not a record: " + this);
  }

  /** If this is an enum, return its symbols. */
  public List<String> getEnumSymbols() {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  /** If this is an enum, return its default value. */
  public String getEnumDefault() {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  /** If this is an enum, return a symbol's ordinal value. */
  public int getEnumOrdinal(String symbol) {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  /** If this is an enum, returns true if it contains given symbol. */
  public boolean hasEnumSymbol(String symbol) {
    throw new AvroRuntimeException("Not an enum: " + this);
  }

  /**
   * If this is a record, enum or fixed, returns its name, otherwise the name of
   * the primitive type.
   */
  public String getName() {
    return type.name;
  }

  /**
   * If this is a record, enum, or fixed, returns its docstring, if available.
   * Otherwise, returns null.
   */
  public String getDoc() {
    return null;
  }

  /** If this is a record, enum or fixed, returns its namespace, if any. */
  public String getNamespace() {
    throw new AvroRuntimeException("Not a named type: " + this);
  }

  /**
   * If this is a record, enum or fixed, returns its namespace-qualified name,
   * otherwise returns the name of the primitive type.
   */
  public String getFullName() {
    return getName();
  }

  /** If this is a record, enum or fixed, add an alias. */
  public void addAlias(String alias) {
    throw new AvroRuntimeException("Not a named type: " + this);
  }

  /** If this is a record, enum or fixed, add an alias. */
  public void addAlias(String alias, String space) {
    throw new AvroRuntimeException("Not a named type: " + this);
  }

  /** If this is a record, enum or fixed, return its aliases, if any. */
  public Set<String> getAliases() {
    throw new AvroRuntimeException("Not a named type: " + this);
  }

  /** Returns true if this record is an error type. */
  public boolean isError() {
    throw new AvroRuntimeException("Not a record: " + this);
  }

  /** If this is an array, returns its element type. */
  public Schema getElementType() {
    throw new AvroRuntimeException("Not an array: " + this);
  }

  /** If this is a map, returns its value type. */
  public Schema getValueType() {
    throw new AvroRuntimeException("Not a map: " + this);
  }

  /** If this is a union, returns its types. */
  public List<Schema> getTypes() {
    throw new AvroRuntimeException("Not a union: " + this);
  }

  /** If this is a union, return the branch with the provided full name. */
  public Integer getIndexNamed(String name) {
    throw new AvroRuntimeException("Not a union: " + this);
  }

  /** If this is fixed, returns its size. */
  public int getFixedSize() {
    throw new AvroRuntimeException("Not fixed: " + this);
  }

  /** Render this as <a href="https://json.org/">JSON</a>. */
  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Render this as <a href="https://json.org/">JSON</a>.
   *
   * @param pretty if true, pretty-print JSON.
   */
  public String toString(boolean pretty) {
    return toString(new Names(), pretty);
  }

  String toString(Names names, boolean pretty) {
    try {
      Object object = toJson(names);
      if (pretty && object instanceof JSONObject) {
        return ((JSONObject) object).toString(2);
      } else if (pretty && object instanceof JSONArray) {
        return ((JSONArray) object).toString(2);
      } else {
        return object.toString();
      }
    } catch (JSONException | RuntimeException e) {
      throw new AvroRuntimeException(e);
    }
  }

  Object toJson(Names names) throws JSONException {
    if (!hasProps()) { // no props defined
      return getName(); // just write name
    } else {
      JSONObject object = new JSONObject();
      object.put("type", getName());
      writeProps(object);
      return object;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof Schema))
      return false;
    return equals((Schema) o, new HashSet<>());
  }

  boolean equals(Schema other, Set<SeenPair> seenPairs) {
    if (getClass() != other.getClass())
      return false;
    if (!(this.type == other.type))
      return false;
    return equalCachedHash(other) && propsEqual(other);
  }


  @Override
  public final int hashCode() {
    if (hashCode == NO_HASHCODE)
      hashCode = computeHash(new HashSet<>());
    return hashCode;
  }

  int computeHash(Set<IdentityReference<Schema>> seen) {
    return getType().hashCode() + propsHashCode();
  }

  final boolean equalCachedHash(Schema other) {
    return (hashCode == other.hashCode) || (hashCode == NO_HASHCODE) || (other.hashCode == NO_HASHCODE);
  }

  private static final Set<String> FIELD_RESERVED = Collections
      .unmodifiableSet(new HashSet<>(Arrays.asList("default", "doc", "name", "order", "type", "aliases")));

  /** Returns true if this record is an union type. */
  public boolean isUnion() {
    return this instanceof UnionSchema;
  }

  /** Returns true if this record is an union type containing null. */
  public boolean isNullable() {
    if (!isUnion()) {
      return getType().equals(Schema.Type.NULL);
    }

    for (Schema schema : getTypes()) {
      if (schema.isNullable()) {
        return true;
      }
    }

    return false;
  }

  /** A field within a record. */
  public static class Field extends JsonProperties {
    /** How values of this field should be ordered when sorting records. */
    public enum Order {
      ASCENDING, DESCENDING, IGNORE;

      private final String name;

      Order() {
        this.name = this.name().toLowerCase(Locale.ENGLISH);
      }
    }

    /**
     * For Schema unions with a "null" type as the first entry, this can be used to
     * specify that the default for the union is null.
     */
    public static final Object NULL_DEFAULT_VALUE = new Object();

    private final String name; // name of the field.
    private int position = -1;
    private final Schema schema;
    private final String doc;
    private final Object defaultValue;
    private final Order order;
    private Set<String> aliases;

    /**
     * Constructs a new Field instance with the same {@code name}, {@code doc},
     * {@code defaultValue}, and {@code order} as {@code field} has with changing
     * the schema to the specified one. It also copies all the {@code props} and
     * {@code aliases}.
     */
    public Field(Field field, Schema schema) throws JSONException {
      this(field.name, schema, field.doc, field.defaultValue, field.order);
      putAll(field);
      if (field.aliases != null)
        aliases = new LinkedHashSet<>(field.aliases);
    }

    /**
     *
     */
    public Field(String name, Schema schema) throws JSONException {
      this(name, schema, null, null, Order.ASCENDING);
    }

    /**
     *
     */
    public Field(String name, Schema schema, String doc) throws JSONException {
      this(name, schema, doc, null, Order.ASCENDING);
    }

    /**
     * @param defaultValue the default value for this field specified using the
     *                     mapping in {@link JsonProperties}
     */
    public Field(String name, Schema schema, String doc, Object defaultValue) throws JSONException {
      this(name, schema, doc,
          defaultValue == NULL_DEFAULT_VALUE ? JSONObject.NULL : defaultValue, Order.ASCENDING);
    }

    /**
     * @param defaultValue the default value for this field specified using the
     *                     mapping in {@link JsonProperties}
     */
    public Field(String name, Schema schema, String doc, Object defaultValue, Order order) throws JSONException {
      super();
      validateName(name);
      this.name = name;
      this.schema = schema;
      this.doc = doc;
      this.defaultValue = validateDefault(name, schema, defaultValue);
      this.order = Objects.requireNonNull(order, "Order cannot be null");
    }

    public String name() {
      return name;
    }

    /** The position of this field within the record. */
    public int pos() {
      return position;
    }

    /** This field's {@link Schema}. */
    public Schema schema() {
      return schema;
    }

    /** Field's documentation within the record, if set. May return null. */
    public String doc() {
      return doc;
    }

    /**
     * @return true if this Field has a default value set. Can be used to determine
     *         if a "null" return from defaultVal() is due to that being the default
     *         value or just not set.
     */
    public boolean hasDefaultValue() {
      return defaultValue != null;
    }

    Object defaultValue() {
      return defaultValue;
    }

    /**
     * @return the default value for this field specified using the mapping in
     *         {@link JsonProperties}
     */
    public Object defaultVal() {
      return defaultValue;
    }

    public Order order() {
      return order;
    }

    public void addAlias(String alias) {
      if (aliases == null)
        this.aliases = new LinkedHashSet<>();
      aliases.add(alias);
    }

    /** Return the defined aliases as an unmodifiable Set. */
    public Set<String> aliases() {
      if (aliases == null)
        return Collections.emptySet();
      return Collections.unmodifiableSet(aliases);
    }

    @Override
    public boolean equals(Object other) {
      if (other == this)
        return true;
      if (!(other instanceof Field))
        return false;
      Field that = (Field) other;
      return name.equals(that.name) && schema.equals(that.schema) && defaultValueEquals(that.defaultValue)
          && order == that.order && propsEqual(that);
    }

    boolean equals(Field other, Set<SeenPair> seenPairs) {
      if (other == this)
        return true;
       return name.equals(other.name) && schema.equals(other.schema, seenPairs) && defaultValueEquals(other.defaultValue)
               && order == other.order && propsEqual(other);
    }

    int computeHash(Set<IdentityReference<Schema>> seen) {
      return name.hashCode() + schema.computeHash(seen);
    }

    @Override
    public int hashCode() {
      return name.hashCode() + schema.hashCode();
    }

    private boolean defaultValueEquals(Object thatDefaultValue) {
      if (defaultValue == null)
        return thatDefaultValue == null;
      if (thatDefaultValue == null)
        return false;
      if (defaultValue instanceof Double && ((Double) defaultValue).isNaN()) {
        return thatDefaultValue instanceof Double && ((Double) thatDefaultValue).isNaN();
      }
      return defaultValue.equals(thatDefaultValue);
    }

    @Override
    public String toString() {
      return name + " type:" + schema.type + " pos:" + position;
    }
  }

  static class Name {
    private final String name;
    private final String space;
    private final String full;

    public Name(String name, String space) {
      if (name == null) { // anonymous
        this.name = this.space = this.full = null;
        return;
      }
      int lastDot = name.lastIndexOf('.');
      if (lastDot < 0) { // unqualified name
        this.name = name;
      } else { // qualified name
        space = name.substring(0, lastDot); // get space from name
        this.name = name.substring(lastDot + 1);
      }
      validateName(this.name);
      if ("".equals(space))
        space = null;
      this.space = space;
      this.full = (this.space == null) ? this.name : this.space + "." + this.name;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof Name))
        return false;
      Name that = (Name) o;
      return Objects.equals(full, that.full);
    }

    @Override
    public int hashCode() {
      return full == null ? 0 : full.hashCode();
    }

    @Override
    public String toString() {
      return full;
    }

    public void writeName(Names names, JSONObject object) throws JSONException {
      if (name != null)
        object.put("name", name);
      if (space != null) {
        if (!space.equals(names.space()))
          object.put("namespace", space);
      } else if (names.space() != null) { // null within non-null
        object.put("namespace", "");
      }
    }

    public String getQualified(String defaultSpace) {
      return (space == null || space.equals(defaultSpace)) ? name : full;
    }
  }

  private static abstract class NamedSchema extends Schema {
    final Name name;
    final String doc;
    Set<Name> aliases;

    public NamedSchema(Type type, Name name, String doc) {
      super(type);
      this.name = name;
      this.doc = doc;
      if (PRIMITIVES.containsKey(name.full)) {
        throw new AvroTypeException("Schemas may not be named after primitives: " + name.full);
      }
    }

    @Override
    public String getName() {
      return name.name;
    }

    @Override
    public String getDoc() {
      return doc;
    }

    @Override
    public String getNamespace() {
      return name.space;
    }

    @Override
    public String getFullName() {
      return name.full;
    }

    @Override
    public void addAlias(String alias) {
      addAlias(alias, null);
    }

    @Override
    public void addAlias(String name, String space) {
      if (aliases == null)
        this.aliases = new LinkedHashSet<>();
      if (space == null)
        space = this.name.space;
      aliases.add(new Name(name, space));
    }

    @Override
    public Set<String> getAliases() {
      Set<String> result = new LinkedHashSet<>();
      if (aliases != null)
        for (Name alias : aliases)
          result.add(alias.full);
      return result;
    }

    public String writeNameRef(Names names) {
      if (this.equals(names.get(name))) {
        return name.getQualified(names.space());
      }
      if (name.name != null) {
        names.put(name, this);
      }
      return null;
    }

    public void writeName(Names names, JSONObject object) throws JSONException {
      name.writeName(names, object);
    }

    public boolean equalNames(NamedSchema that) {
      return this.name.equals(that.name);
    }

    @Override
    int computeHash(Set<IdentityReference<Schema>> seen) {
      return super.computeHash(seen) + name.hashCode();
    }

    public void aliasesToJson(JSONObject object) throws JSONException {
      if (aliases == null || aliases.size() == 0)
        return;
      JSONArray array = new JSONArray();
      for (Name alias : aliases)
        array.put(alias.getQualified(name.space));
      object.put("aliases", array);
    }

  }

  /**
   * Useful as key of {@link Map}s when traversing two schemas at the same time
   * and need to watch for recursion.
   */
  public static class SeenPair {
    private final Object s1;
    private final Object s2;

    public SeenPair(Object s1, Object s2) {
      this.s1 = s1;
      this.s2 = s2;
    }

    public boolean equals(Object o) {
      if (!(o instanceof SeenPair))
        return false;
      return this.s1 == ((SeenPair) o).s1 && this.s2 == ((SeenPair) o).s2;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(s1) + System.identityHashCode(s2);
    }
  }

  @SuppressWarnings(value = "unchecked")
  private static class RecordSchema extends NamedSchema {
    private List<Field> fields;
    private Map<String, Field> fieldMap;
    private final boolean isError;

    public RecordSchema(Name name, String doc, boolean isError) {
      super(Type.RECORD, name, doc);
      this.isError = isError;
    }

    @Override
    public boolean isError() {
      return isError;
    }

    @Override
    public Field getField(String fieldname) {
      if (fieldMap == null)
        throw new AvroRuntimeException("Schema fields not set yet");
      return fieldMap.get(fieldname);
    }

    @Override
    public List<Field> getFields() {
      if (fields == null)
        throw new AvroRuntimeException("Schema fields not set yet");
      return fields;
    }

    @Override
    public void setFields(List<Field> fields) {
      if (this.fields != null) {
        throw new AvroRuntimeException("Fields are already set");
      }
      int i = 0;
      fieldMap = new HashMap<>(2 * fields.size());
      List<Field> ff = new ArrayList<>(fields.size());
      for (Field f : fields) {
        if (f.position != -1) {
          throw new AvroRuntimeException("Field already used: " + f);
        }
        f.position = i++;
        final Field existingField = fieldMap.put(f.name(), f);
        if (existingField != null) {
          throw new AvroRuntimeException(
              String.format("Duplicate field %s in record %s: %s and %s.", f.name(), name, f, existingField));
        }
        ff.add(f);
      }
      this.fields = Collections.unmodifiableList(ff);
      this.hashCode = NO_HASHCODE;
    }

    boolean equals(Schema o, Set<SeenPair> seenPairs) {
      if (o == this)
        return true;
      if (!(o instanceof RecordSchema))
        return false;
      RecordSchema that = (RecordSchema) o;
      if (!equalCachedHash(that))
        return false;
      if (!equalNames(that))
        return false;
      if (!propsEqual(that))
        return false;
      SeenPair here = new SeenPair(this, o);

      if (seenPairs.contains(here))
        return true; // prevent stack overflow
      seenPairs.add(here);
      if (fields.size() != that.fields.size())
        return false;
      for (int i = 0; i < fields.size(); i++) {
        if (!fields.get(i).equals(that.fields.get(i), seenPairs))
          return false;
      }
      return true;
    }

    @Override
    int computeHash(Set<IdentityReference<Schema>> seen) {
      if (seen.add(new IdentityReference<>(this))) {
        int hashCode = super.computeHash(seen);
        for (Field field : fields) {
          hashCode = 31 * hashCode + field.computeHash(seen);
        }
        return hashCode;
      } else {
        return 0;  // prevent stack overflow
      }
    }

    @Override
    Object toJson(Names names) throws JSONException {
      String nameRef = writeNameRef(names);
      if (nameRef != null)
        return nameRef;
      String savedSpace = names.space; // save namespace
      JSONObject object = new JSONObject();
      object.put("type", isError ? "error" : "record");
      writeName(names, object);
      names.space = name.space; // set default namespace
      if (getDoc() != null)
        object.put("doc", getDoc());

      if (fields != null) {
        object.put("fields", fieldsToJson(names));
      }

      writeProps(object);
      aliasesToJson(object);
      names.space = savedSpace; // restore namespace
      return object;
    }

    JSONArray fieldsToJson(Names names) throws JSONException {
      JSONArray array = new JSONArray();
      for (Field f : fields) {
        JSONObject fieldObject = new JSONObject();
        fieldObject.put("name", f.name());
        fieldObject.put("type", f.schema.toJson(names));
        if (f.doc() != null)
          fieldObject.put("doc", f.doc());
        if (f.hasDefaultValue()) {
          fieldObject.put("default", f.defaultValue());
        }
        if (f.order() != Field.Order.ASCENDING)
          fieldObject.put("order", f.order().name);
        if (f.aliases != null && f.aliases.size() != 0) {
          fieldObject.put("aliases", new JSONArray(f.aliases));
        }
        f.writeProps(fieldObject);
        array.put(fieldObject);
      }
      return array;
    }
  }

  private static class EnumSchema extends NamedSchema {
    private final List<String> symbols;
    private final Map<String, Integer> ordinals;
    private final String enumDefault;

    public EnumSchema(Name name, String doc, List<String> symbols, String enumDefault) {
      super(Type.ENUM, name, doc);
      this.symbols = symbols;
      this.ordinals = new HashMap<>(2 * symbols.size());
      this.enumDefault = enumDefault;
      int i = 0;
      for (String symbol : symbols) {
        validateName(symbol);
        if (ordinals.put(symbol, i++) != null) {
          throw new SchemaParseException("Duplicate enum symbol: " + symbol);
        }
      }
      if (enumDefault != null && !symbols.contains(enumDefault)) {
        throw new SchemaParseException(
            "The Enum Default: " + enumDefault + " is not in the enum symbol set: " + symbols);
      }
    }

    @Override
    public List<String> getEnumSymbols() {
      return symbols;
    }

    @Override
    public boolean hasEnumSymbol(String symbol) {
      return ordinals.containsKey(symbol);
    }

    @Override
    public int getEnumOrdinal(String symbol) {
      return ordinals.get(symbol);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof EnumSchema))
        return false;
      EnumSchema that = (EnumSchema) o;
      return equalCachedHash(that) && equalNames(that) && symbols.equals(that.symbols) && propsEqual(that);
    }

    @Override
    public String getEnumDefault() {
      return enumDefault;
    }

    @Override
    int computeHash(Set<IdentityReference<Schema>> seen) {
      return super.computeHash(seen) + symbols.hashCode();
    }

    @Override
    Object toJson(Names names) throws JSONException {
      String nameRef = writeNameRef(names);
      if (nameRef != null)
        return nameRef;

      JSONObject object = new JSONObject();
      object.put("type", "enum");
      writeName(names, object);
      if (getDoc() != null)
        object.put("doc", getDoc());
      object.put("symbols", new JSONArray(symbols));
      if (getEnumDefault() != null)
        object.put("default", getEnumDefault());
      writeProps(object);
      aliasesToJson(object);
      return object;
    }
  }

  private static class ArraySchema extends Schema {
    private final Schema elementType;

    public ArraySchema(Schema elementType) {
      super(Type.ARRAY);
      this.elementType = elementType;
    }

    @Override
    public Schema getElementType() {
      return elementType;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof ArraySchema))
        return false;
      ArraySchema that = (ArraySchema) o;
      return equalCachedHash(that) && elementType.equals(that.elementType) && propsEqual(that);
    }

    @Override
    int computeHash(Set<IdentityReference<Schema>> seen) {
      return super.computeHash(seen) + elementType.computeHash(seen);
    }

    @Override
    Object toJson(Names names) throws JSONException {
      JSONObject object = new JSONObject();
      object.put("type", "array");
      object.put("items", elementType.toJson(names));
      writeProps(object);
      return object;
    }
  }

  private static class MapSchema extends Schema {
    private final Schema valueType;

    public MapSchema(Schema valueType) {
      super(Type.MAP);
      this.valueType = valueType;
    }

    @Override
    public Schema getValueType() {
      return valueType;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof MapSchema))
        return false;
      MapSchema that = (MapSchema) o;
      return equalCachedHash(that) && valueType.equals(that.valueType) && propsEqual(that);
    }

    @Override
    int computeHash(Set<IdentityReference<Schema>> seen) {
      return super.computeHash(seen) + valueType.computeHash(seen);
    }

    @Override
    Object toJson(Names names) throws JSONException {
      JSONObject object = new JSONObject();
      object.put("type", "map");
      object.put("values", valueType.toJson(names));
      writeProps(object);
      return object;
    }
  }

  private static class UnionSchema extends Schema {
    private final List<Schema> types;
    private final Map<String, Integer> indexByName;

    public UnionSchema(List<Schema> types) {
      super(Type.UNION);
      this.indexByName = new HashMap<>(2 * types.size());
      this.types = Collections.unmodifiableList(new ArrayList<>(types));
      int index = 0;
      for (Schema type : types) {
        if (type.getType() == Type.UNION) {
          throw new AvroRuntimeException("Nested union: " + this);
        }
        String name = type.getFullName();
        if (name == null) {
          throw new AvroRuntimeException("Nameless in union:" + this);
        }
        if (indexByName.put(name, index++) != null) {
          throw new AvroRuntimeException("Duplicate in union:" + name);
        }
      }
    }

    @Override
    public List<Schema> getTypes() {
      return types;
    }

    @Override
    public Integer getIndexNamed(String name) {
      return indexByName.get(name);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof UnionSchema))
        return false;
      UnionSchema that = (UnionSchema) o;
      return equalCachedHash(that) && types.equals(that.types) && propsEqual(that);
    }

    @Override
    int computeHash(Set<IdentityReference<Schema>> seen) {
      int hash = super.computeHash(seen);
      for (Schema type : types)
        hash = 31 * hash + type.computeHash(seen);
      return hash;
    }

    @Override
    Object toJson(Names names) throws JSONException {
      JSONArray array = new JSONArray();
      for (Schema type : types)
        array.put(type.toJson(names));
      return array;
    }
  }

  private static class FixedSchema extends NamedSchema {
    private final int size;

    public FixedSchema(Name name, String doc, int size) {
      super(Type.FIXED, name, doc);
      if (size < 0)
        throw new IllegalArgumentException("Invalid fixed size: " + size);
      this.size = size;
    }

    @Override
    public int getFixedSize() {
      return size;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof FixedSchema))
        return false;
      FixedSchema that = (FixedSchema) o;
      return equalCachedHash(that) && equalNames(that) && size == that.size && propsEqual(that);
    }

    @Override
    int computeHash(Set<IdentityReference<Schema>> seen) {
      return super.computeHash(seen) + size;
    }

    @Override
    Object toJson(Names names) throws JSONException {
      String nameRef = writeNameRef(names);
      if (nameRef != null)
        return nameRef;

      JSONObject object = new JSONObject();
      object.put("type", "fixed");
      writeName(names, object);
      if (getDoc() != null)
        object.put("doc", getDoc());
      object.put("size", size);
      writeProps(object);
      aliasesToJson(object);
      return object;
    }
  }

  private static class StringSchema extends Schema {
    public StringSchema() {
      super(Type.STRING);
    }
  }

  private static class BytesSchema extends Schema {
    public BytesSchema() {
      super(Type.BYTES);
    }
  }

  private static class IntSchema extends Schema {
    public IntSchema() {
      super(Type.INT);
    }
  }

  private static class LongSchema extends Schema {
    public LongSchema() {
      super(Type.LONG);
    }
  }

  private static class FloatSchema extends Schema {
    public FloatSchema() {
      super(Type.FLOAT);
    }
  }

  private static class DoubleSchema extends Schema {
    public DoubleSchema() {
      super(Type.DOUBLE);
    }
  }

  private static class BooleanSchema extends Schema {
    public BooleanSchema() {
      super(Type.BOOLEAN);
    }
  }

  private static class NullSchema extends Schema {
    public NullSchema() {
      super(Type.NULL);
    }
  }

  /**
   * A parser for JSON-format schemas. Each named schema parsed with a parser is
   * added to the names known to the parser so that subsequently parsed schemas
   * may refer to it by name.
   */
  public static class Parser {
    private final Names names = new Names();

    /**
     * Parse a schema from the provided file. If named, the schema is added to the
     * names known to this parser.
     */
    public Schema parse(File file) throws IOException {
      try (FileInputStream inputStream = new FileInputStream(file)) {
        return parse(inputStream);
      }
    }

    /**
     * Parse a schema from the provided stream. If named, the schema is added to the
     * names known to this parser. The input stream stays open after the parsing.
     */
    public Schema parse(InputStream in) throws IOException {
      InputStreamReader reader = new InputStreamReader(in);
      char[] buffer = new char[8192];
      StringBuilder builder = new StringBuilder();
      int numRead = reader.read(buffer);
      while (numRead != -1) {
        builder.append(buffer, 0, numRead);
        numRead = reader.read(buffer);
      }
      return parse(builder.toString());
    }

    /**
     * Parse a schema from the provided string. If named, the schema is added to the
     * names known to this parser.
     */
    public Schema parse(String s) {
      try {
        return parse(new JSONObject(s));
      } catch (JSONException e) {
        throw new SchemaParseException(e);
      }
    }

    private Schema parse(JSONObject parser) {
      try {
        return Schema.parse(parser, names);
      } catch (JSONException e) {
        throw new SchemaParseException(e);
      }
    }
  }

  static final Map<String, Type> PRIMITIVES = new HashMap<>();
  static {
    PRIMITIVES.put("string", Type.STRING);
    PRIMITIVES.put("bytes", Type.BYTES);
    PRIMITIVES.put("int", Type.INT);
    PRIMITIVES.put("long", Type.LONG);
    PRIMITIVES.put("float", Type.FLOAT);
    PRIMITIVES.put("double", Type.DOUBLE);
    PRIMITIVES.put("boolean", Type.BOOLEAN);
    PRIMITIVES.put("null", Type.NULL);
  }

  static class Names extends LinkedHashMap<Name, Schema> {
    private static final long serialVersionUID = 1L;
    private String space; // default namespace

    public Names() {
    }

    public String space() {
      return space;
    }

    public void space(String space) {
      this.space = space;
    }

    public Schema get(String o) {
      Type primitive = PRIMITIVES.get(o);
      if (primitive != null) {
        return Schema.create(primitive);
      }
      Name name = new Name(o, space);
      if (!containsKey(name)) {
        // if not in default try anonymous
        name = new Name(o, "");
      }
      return super.get(name);
    }

    public void add(Schema schema) {
      put(((NamedSchema) schema).name, schema);
    }

    @Override
    public Schema put(Name name, Schema schema) {
      if (containsKey(name))
        throw new SchemaParseException("Can't redefine: " + name);
      return super.put(name, schema);
    }
  }

  private static void validateName(String name) {
    if (name == null)
      throw new SchemaParseException("Null name");
    int length = name.length();
    if (length == 0)
      throw new SchemaParseException("Empty name");
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_'))
      throw new SchemaParseException("Illegal initial character: " + name);
    for (int i = 1; i < length; i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_'))
        throw new SchemaParseException("Illegal character in: " + name);
    }
  }

  private static Object validateDefault(String fieldName, Schema schema, Object defaultValue) throws JSONException {
    if ((defaultValue != null) && !isValidDefault(schema, defaultValue)) { // invalid default
      String message = "Invalid default for field " + fieldName + ": " + defaultValue + " not a " + schema;
      throw new AvroTypeException(message); // throw exception
    }
    return defaultValue;
  }

  private static boolean isValidDefault(Schema schema, Object defaultValue) throws JSONException {
    if (defaultValue == null)
      return false;
    switch (schema.getType()) {
    case STRING:
    case BYTES:
    case ENUM:
    case FIXED:
      return defaultValue instanceof String;
    case INT:
    case LONG:
      return defaultValue instanceof Long;
    case FLOAT:
    case DOUBLE:
      return defaultValue instanceof Double;
    case BOOLEAN:
      return defaultValue instanceof Boolean;
    case NULL:
      return defaultValue == JSONObject.NULL;
    case ARRAY:
      if (!(defaultValue instanceof JSONArray))
        return false;
      int arrayLengh = ((JSONArray) defaultValue).length();
      for (int i = 0; i < arrayLengh; i++)
        if (!isValidDefault(schema.getElementType(), ((JSONArray) defaultValue).opt(i)))
          return false;
      return true;
    case MAP:
      if (!(defaultValue instanceof JSONObject))
        return false;
      for (Iterator<String> it = ((JSONObject) defaultValue).keys(); it.hasNext(); ) {
        String key = it.next();
        if (!isValidDefault(schema.getValueType(), ((JSONObject) defaultValue).opt(key)))
          return false;
      }
      return true;
    case UNION: // union default: first branch
      return isValidDefault(schema.getTypes().get(0), defaultValue);
    case RECORD:
      if (!(defaultValue instanceof JSONObject))
        return false;
      for (Field field : schema.getFields())
        if (!isValidDefault(field.schema(),
            ((JSONObject)defaultValue).has(field.name()) ? ((JSONObject)defaultValue).get(field.name()) : field.defaultValue()))
          return false;
      return true;
    default:
      return false;
    }
  }

  static Schema parse(Object schema, Names names) throws JSONException {
    if (schema == null) {
      throw new SchemaParseException("Cannot parse <null> schema");
    }
    if (schema instanceof String) { // name
      Schema result = names.get((String) schema);
      if (result == null)
        throw new SchemaParseException("Undefined name: " + schema);
      return result;
    } else if (schema instanceof JSONObject) {
      JSONObject schemaObject = (JSONObject) schema;
      Schema result;
      String type = getRequiredText(schemaObject, "type", "No type");
      Name name = null;
      String savedSpace = names.space();
      String doc = null;
      if (type.equals("record") || type.equals("error") || type.equals("enum") || type.equals("fixed")) {
        String space = getOptionalText(schemaObject, "namespace");
        doc = getOptionalText(schemaObject, "doc");
        if (space == null)
          space = names.space();
        name = new Name(getRequiredText(schemaObject, "name", "No name in schema"), space);
        names.space(name.space); // set default namespace
      }
      Type primitive = PRIMITIVES.get(type);
      if (primitive != null) { // primitive
        result = create(primitive);
      } else if (type.equals("record") || type.equals("error")) { // record
        List<Field> fields = new ArrayList<>();
        result = new RecordSchema(name, doc, type.equals("error"));
        names.add(result);
        JSONArray fieldsArray = schemaObject.getJSONArray("fields");
        int fieldsLength = fieldsArray.length();
        for (int i = 0; i < fieldsLength; i++) {
          JSONObject fieldObject = fieldsArray.getJSONObject(i);
          String fieldName = getRequiredText(fieldObject, "name", "No field name");
          String fieldDoc = getOptionalText(fieldObject, "doc");
          Object fieldTypeNode = fieldObject.get("type");
          if (fieldTypeNode instanceof String && names.get((String)fieldTypeNode) == null)
            throw new SchemaParseException(fieldTypeNode + " is not a defined name." + " The type of the \"" + fieldName
                + "\" field must be" + " a defined name or a {\"type\": ...} expression.");

          Schema fieldSchema = parse(fieldTypeNode, names);
          Field.Order order = Field.Order.ASCENDING;
          String orderNode = fieldObject.optString("order");
          if (!orderNode.equals(""))
            order = Field.Order.valueOf(orderNode.toUpperCase(Locale.ENGLISH));
          Object defaultValue = fieldObject.opt("default");
          if (defaultValue != null
              && (Type.FLOAT.equals(fieldSchema.getType()) || Type.DOUBLE.equals(fieldSchema.getType()))
              && defaultValue instanceof String)
            defaultValue = Double.valueOf((String) defaultValue);
          Field f = new Field(fieldName, fieldSchema, fieldDoc, defaultValue, order);
          Iterator<String> keys = fieldObject.keys();
          while (keys.hasNext()) { // add field props
            String prop = keys.next();
            if (!FIELD_RESERVED.contains(prop))
              f.addProp(prop, fieldObject.get(prop));
          }
          f.aliases = parseAliases(fieldObject);
          fields.add(f);
        }
        result.setFields(fields);
      } else if (type.equals("enum")) { // enum
        JSONArray symbolsNode = ((JSONObject) schema).getJSONArray("symbols");
        int symbolsLength = symbolsNode.length();
        List<String> symbols = new ArrayList<>(symbolsLength);
        for (int i = 0; i < symbolsLength; i++) {
          symbols.add(symbolsNode.getString(i));
        }

        String enumDefault = schemaObject.optString("default");
        String defaultSymbol = null;
        if (!enumDefault.equals(""))
          defaultSymbol = enumDefault;
        result = new EnumSchema(name, doc, symbols, defaultSymbol);
        names.add(result);
      } else if (type.equals("array")) { // array
        Object itemsNode = schemaObject.get("items");
        result = new ArraySchema(parse(itemsNode, names));
      } else if (type.equals("map")) { // map
        Object valuesNode = schemaObject.get("values");
        result = new MapSchema(parse(valuesNode, names));
      } else if (type.equals("fixed")) { // fixed
        int sizeNode = ((JSONObject) schema).getInt("size");
        result = new FixedSchema(name, doc, sizeNode);
        names.add(result);
      } else { // For unions with self reference
        Name nameFromType = new Name(type, names.space);
        if (names.containsKey(nameFromType)) {
          return names.get(nameFromType);
        }
        throw new SchemaParseException("Type not supported: " + type);
      }
      Iterator<String> i = schemaObject.keys();

      Set<String> reserved = SCHEMA_RESERVED;
      if (type.equals("enum")) {
        reserved = ENUM_RESERVED;
      }
      while (i.hasNext()) { // add properties
        String prop = i.next();
        if (!reserved.contains(prop)) // ignore reserved
          result.addProp(prop, schemaObject.get(prop));
      }
      // parse logical type if present
      names.space(savedSpace); // restore space
      if (result instanceof NamedSchema) {
        Set<String> aliases = parseAliases(schemaObject);
        if (aliases != null) // add aliases
          for (String alias : aliases)
            result.addAlias(alias);
      }
      return result;
    } else if (schema instanceof JSONArray) { // union
      JSONArray schemaArray = (JSONArray) schema;
      List<Schema> types = new ArrayList<>(schemaArray.length());
      for (int i = 0; i < schemaArray.length(); i++) {
        types.add(parse(schemaArray.get(i), names));
      }
      return new UnionSchema(types);
    } else {
      throw new SchemaParseException("Schema not yet supported: " + schema);
    }
  }

  static Set<String> parseAliases(JSONObject node) throws JSONException {
    JSONArray aliasesNode = node.optJSONArray("aliases");
    if (aliasesNode == null)
      return null;
    Set<String> aliases = new LinkedHashSet<>();
    int aliasesLength = aliasesNode.length();
    for (int i = 0; i < aliasesLength; i++) {
      aliases.add(aliasesNode.getString(i));
    }
    return aliases;
  }

  /**
   * Extracts text value associated to key from the container JsonNode, and throws
   * {@link SchemaParseException} if it doesn't exist.
   *
   * @param container Container where to find key.
   * @param key       Key to look for in container.
   * @param error     String to prepend to the SchemaParseException.
   */
  private static String getRequiredText(JSONObject container, String key, String error) {
    String out = getOptionalText(container, key);
    if (null == out) {
      throw new SchemaParseException(error + ": " + container);
    }
    return out;
  }

  /** Extracts text value associated to key from the container JsonNode. */
  private static String getOptionalText(JSONObject container, String key) {
    Object value = container.opt(key);
    if (value instanceof String) {
      return (String) value;
    } else {
      return null;
    }
  }

  /**
   * Rewrite a writer's schema using the aliases from a reader's schema. This
   * permits reading records, enums and fixed schemas whose names have changed,
   * and records whose field names have changed. The returned schema always
   * contains the same data elements in the same order, but with possibly
   * different names.
   */
  public static Schema applyAliases(Schema writer, Schema reader) {
    if (writer.equals(reader))
      return writer; // same schema

    // create indexes of names
    Map<Schema, Schema> seen = new IdentityHashMap<>(1);
    Map<Name, Name> aliases = new HashMap<>(1);
    Map<Name, Map<String, String>> fieldAliases = new HashMap<>(1);
    getAliases(reader, seen, aliases, fieldAliases);

    if (aliases.size() == 0 && fieldAliases.size() == 0)
      return writer; // no aliases

    seen.clear();
    try {
      return applyAliases(writer, seen, aliases, fieldAliases);
    } catch (JSONException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  private static Schema applyAliases(Schema s, Map<Schema, Schema> seen, Map<Name, Name> aliases,
                                     Map<Name, Map<String, String>> fieldAliases) throws JSONException {

    Name name = s instanceof NamedSchema ? ((NamedSchema) s).name : null;
    Schema result = s;
    switch (s.getType()) {
    case RECORD:
      if (seen.containsKey(s))
        return seen.get(s); // break loops

      Name alias = aliases.get(name);
      if (alias != null)
        name = alias;
      result = Schema.createRecord(name.full, s.getDoc(), null, s.isError());
      seen.put(s, result);
      List<Field> newFields = new ArrayList<>();
      for (Field f : s.getFields()) {
        Schema fSchema = applyAliases(f.schema, seen, aliases, fieldAliases);
        String fName = getFieldAlias(name, f.name, fieldAliases);
        Field newF = new Field(fName, fSchema, f.doc, f.defaultValue, f.order);
        newF.putAll(f); // copy props
        newFields.add(newF);
      }
      result.setFields(newFields);
      break;
    case ENUM:
      if (aliases.containsKey(name))
        result = Schema.createEnum(aliases.get(name).full, s.getDoc(), null, s.getEnumSymbols(), s.getEnumDefault());
      break;
    case ARRAY:
      Schema e = applyAliases(s.getElementType(), seen, aliases, fieldAliases);
      if (!e.equals(s.getElementType()))
        result = Schema.createArray(e);
      break;
    case MAP:
      Schema v = applyAliases(s.getValueType(), seen, aliases, fieldAliases);
      if (!v.equals(s.getValueType()))
        result = Schema.createMap(v);
      break;
    case UNION:
      List<Schema> types = new ArrayList<>();
      for (Schema branch : s.getTypes())
        types.add(applyAliases(branch, seen, aliases, fieldAliases));
      result = Schema.createUnion(types);
      break;
    case FIXED:
      if (aliases.containsKey(name))
        result = Schema.createFixed(aliases.get(name).full, s.getDoc(), null, s.getFixedSize());
      break;
    default:
      // NO-OP
    }
    if (!result.equals(s))
      result.putAll(s); // copy props
    return result;
  }

  private static void getAliases(Schema schema, Map<Schema, Schema> seen, Map<Name, Name> aliases,
      Map<Name, Map<String, String>> fieldAliases) {
    if (schema instanceof NamedSchema) {
      NamedSchema namedSchema = (NamedSchema) schema;
      if (namedSchema.aliases != null)
        for (Name alias : namedSchema.aliases)
          aliases.put(alias, namedSchema.name);
    }
    switch (schema.getType()) {
    case RECORD:
      if (seen.containsKey(schema))
        return; // break loops
      seen.put(schema, schema);
      RecordSchema record = (RecordSchema) schema;
      for (Field field : schema.getFields()) {
        if (field.aliases != null && !field.aliases.isEmpty()) {
          Map<String, String> recordAliases = fieldAliases.get(record.name);
          if (recordAliases == null) {
            recordAliases = new HashMap<>();
            fieldAliases.put(record.name, recordAliases);
          }
          for (String fieldAlias : field.aliases) {
            recordAliases.put(fieldAlias, field.name);
          }
        }
        getAliases(field.schema, seen, aliases, fieldAliases);
      }
      if (record.aliases != null && fieldAliases.containsKey(record.name))
        for (Name recordAlias : record.aliases)
          fieldAliases.put(recordAlias, fieldAliases.get(record.name));
      break;
    case ARRAY:
      getAliases(schema.getElementType(), seen, aliases, fieldAliases);
      break;
    case MAP:
      getAliases(schema.getValueType(), seen, aliases, fieldAliases);
      break;
    case UNION:
      for (Schema s : schema.getTypes())
        getAliases(s, seen, aliases, fieldAliases);
      break;
    }
  }

  private static String getFieldAlias(Name record, String field, Map<Name, Map<String, String>> fieldAliases) {
    Map<String, String> recordAliases = fieldAliases.get(record);
    if (recordAliases == null)
      return field;
    String alias = recordAliases.get(field);
    if (alias == null)
      return field;
    return alias;
  }
}
