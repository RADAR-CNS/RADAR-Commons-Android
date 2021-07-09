package org.apache.avro.io.parsing;

public final class Symbols {
    /**
     * The terminal symbols for the grammar.
     */
    public static final Symbol NULL = new Symbol.Terminal("null");
    public static final Symbol BOOLEAN = new Symbol.Terminal("boolean");
    public static final Symbol INT = new Symbol.Terminal("int");
    public static final Symbol LONG = new Symbol.Terminal("long");
    public static final Symbol FLOAT = new Symbol.Terminal("float");
    public static final Symbol DOUBLE = new Symbol.Terminal("double");
    public static final Symbol STRING = new Symbol.Terminal("string");
    public static final Symbol BYTES = new Symbol.Terminal("bytes");
    public static final Symbol FIXED = new Symbol.Terminal("fixed");
    public static final Symbol ENUM = new Symbol.Terminal("enum");
    public static final Symbol UNION = new Symbol.Terminal("union");
    public static final Symbol ARRAY_START = new Symbol.Terminal("array-start");
    public static final Symbol ARRAY_END = new Symbol.Terminal("array-end");
    public static final Symbol MAP_START = new Symbol.Terminal("map-start");
    public static final Symbol MAP_END = new Symbol.Terminal("map-end");
    public static final Symbol ITEM_END = new Symbol.Terminal("item-end");
    public static final Symbol WRITER_UNION_ACTION = new Symbol.WriterUnionAction();
    /* a pseudo terminal used by parsers */
    public static final Symbol FIELD_ACTION = new Symbol.Terminal("field-action");
    public static final Symbol RECORD_START = new Symbol.ImplicitAction("record-start", false);
    public static final Symbol RECORD_END = new Symbol.ImplicitAction("record-end", true);
    public static final Symbol UNION_END = new Symbol.ImplicitAction("union-end", true);
    public static final Symbol FIELD_END = new Symbol.ImplicitAction("field-end", true);
    public static final Symbol DEFAULT_END_ACTION = new Symbol.ImplicitAction("default-end", true);
    public static final Symbol MAP_KEY_MARKER = new Symbol.Terminal("map-key-marker");

    private Symbols() { }
}
