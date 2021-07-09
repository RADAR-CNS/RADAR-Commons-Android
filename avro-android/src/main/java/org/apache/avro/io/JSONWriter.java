package org.apache.avro.io;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implements {@link JSONObject#toString} and {@link JSONArray#toString}. Most
 * application developers should use those methods directly and disregard this
 * API. For example:<pre>
 * JSONObject object = ...
 * String json = object.toString();</pre>
 *
 * <p>Stringers only encode well-formed JSON strings. In particular:
 * <ul>
 *   <li>The stringer must have exactly one top-level array or object.
 *   <li>Lexical scopes must be balanced: every call to {@link #array} must
 *       have a matching call to {@link #endArray} and every call to {@link
 *       #object} must have a matching call to {@link #endObject}.
 *   <li>Arrays may not contain keys (property names).
 *   <li>Objects must alternate keys (property names) and values.
 *   <li>Values are inserted with either literal {@link #value(Object) value}
 *       calls, or by nesting arrays or objects.
 * </ul>
 * Calls that would result in a malformed JSON string will fail with a
 * {@link JSONException}.
 *
 * <p>This class provides no facility for pretty-printing (ie. indenting)
 * output. To encode indented output, use {@link JSONObject#toString(int)} or
 * {@link JSONArray#toString(int)}.
 *
 * <p>Some implementations of the API support at most 20 levels of nesting.
 * Attempts to create more than 20 levels of nesting may fail with a {@link
 * JSONException}.
 *
 * <p>Each stringer may be used to encode a single top level value. Instances of
 * this class are not thread safe. Although this class is nonfinal, it was not
 * designed for inheritance and should not be subclassed. In particular,
 * self-use by overrideable methods is not specified. See <i>Effective Java</i>
 * Item 17, "Design and Document or inheritance or else prohibit it" for further
 * information.
 */
public class JSONWriter {
    /**
     * Lexical scoping elements within this stringer, necessary to insert the
     * appropriate separator characters (ie. commas and colons) and to detect
     * nesting errors.
     */
    enum Scope {

        /**
         * An array with no elements requires no separators or newlines before
         * it is closed.
         */
        EMPTY_ARRAY,

        /**
         * A array with at least one value requires a comma and newline before
         * the next element.
         */
        NONEMPTY_ARRAY,

        /**
         * An object with no keys or values requires no separators or newlines
         * before it is closed.
         */
        EMPTY_OBJECT,

        /**
         * An object whose most recent element is a key. The next element must
         * be a value.
         */
        DANGLING_KEY,

        /**
         * An object with at least one name/value pair requires a comma and
         * newline before the next element.
         */
        NONEMPTY_OBJECT,

        /**
         * A special bracketless array needed by JSONStringer.join() and
         * JSONObject.quote() only. Not used for JSON encoding.
         */
        NULL,
    }

    /**
     * Unlike the original implementation, this stack isn't limited to 20
     * levels of nesting.
     */
    private final List<Scope> stack = new ArrayList<>();
    private final Appendable out;
    private boolean isEmpty = true;

    /**
     * A string containing a full set of spaces for a single level of
     * indentation, or null for no pretty printing.
     */
    private final String indent;

    public JSONWriter(Appendable appendable) {
        this.out = appendable;
        indent = null;
    }

    /**
     * Begins encoding a new array. Each call to this method must be paired with
     * a call to {@link #endArray}.
     *
     * @return this stringer.
     */
    public JSONWriter array() throws JSONException, IOException {
        return open(Scope.EMPTY_ARRAY, "[");
    }

    /**
     * Ends encoding the current array.
     *
     * @return this stringer.
     */
    public JSONWriter endArray() throws JSONException, IOException {
        return close(Scope.EMPTY_ARRAY, Scope.NONEMPTY_ARRAY, "]");
    }

    /**
     * Begins encoding a new object. Each call to this method must be paired
     * with a call to {@link #endObject}.
     *
     * @return this stringer.
     */
    public JSONWriter object() throws JSONException, IOException {
        return open(Scope.EMPTY_OBJECT, "{");
    }

    /**
     * Ends encoding the current object.
     *
     * @return this stringer.
     */
    public JSONWriter endObject() throws JSONException, IOException {
        return close(Scope.EMPTY_OBJECT, Scope.NONEMPTY_OBJECT, "}");
    }

    /**
     * Enters a new scope by appending any necessary whitespace and the given
     * bracket.
     */
    JSONWriter open(Scope empty, String openBracket) throws JSONException, IOException {
        if (stack.isEmpty() && !isEmpty) {
            throw new JSONException("Nesting problem: multiple top-level roots");
        }
        beforeValue();
        stack.add(empty);
        append(openBracket);
        return this;
    }

    private void append(char c) throws IOException {
        isEmpty = false;
        out.append(c);
    }

    private void append(CharSequence csq) throws IOException {
        isEmpty = false;
        out.append(csq);
    }

    /**
     * Closes the current scope by appending any necessary whitespace and the
     * given bracket.
     */
    JSONWriter close(Scope empty, Scope nonempty, String closeBracket) throws JSONException, IOException {
        Scope context = peek();
        if (context != nonempty && context != empty) {
            throw new JSONException("Nesting problem");
        }

        stack.remove(stack.size() - 1);
        if (context == nonempty) {
            newline();
        }
        append(closeBracket);
        return this;
    }

    /**
     * Returns the value on the top of the stack.
     */
    private Scope peek() throws JSONException {
        if (stack.isEmpty()) {
            throw new JSONException("Nesting problem");
        }
        return stack.get(stack.size() - 1);
    }

    /**
     * Replace the value on the top of the stack with the given value.
     */
    private void replaceTop(Scope topOfStack) {
        stack.set(stack.size() - 1, topOfStack);
    }

    /**
     * Encodes {@code value}.
     *
     * @param value a {@link JSONObject}, {@link JSONArray}, String, Boolean,
     *     Integer, Long, Double or null. May not be {@link Double#isNaN() NaNs}
     *     or {@link Double#isInfinite() infinities}.
     * @return this stringer.
     */
    public JSONWriter value(Object value) throws JSONException, IOException {
        if (stack.isEmpty()) {
            throw new JSONException("Nesting problem");
        }

        if (value instanceof JSONArray) {
            array();
            JSONArray array = (JSONArray) value;
            for (int i = 0; i < array.length(); i++) {
                value(array.get(i));
            }
            endArray();
            return this;
        } else if (value instanceof JSONObject) {
            object();
            JSONObject object = (JSONObject) value;
            for (Iterator<String> it = object.keys(); it.hasNext(); ) {
                String key = it.next();
                key(key).value(object.get(key));
            }
            endObject();
            return this;
        }

        beforeValue();

        if (value == null || value == JSONObject.NULL) {
            append("null");
        } else if (value instanceof Boolean) {
            append(value.toString());
        } else if (value instanceof Double) {
            if (((Double) value).isNaN() || ((Double) value).isInfinite()) {
                throw new JSONException("Cannot encode non-number doubles");
            }
            append(value.toString());
        } else if (value instanceof Float) {
            if (((Float) value).isNaN() || ((Float) value).isInfinite()) {
                throw new JSONException("Cannot encode non-number floats");
            }
            append(value.toString());
        } else if (value instanceof Number) {
            append(value.toString());
        } else {
            string(value.toString());
        }

        return this;
    }

    /**
     * Encodes {@code value} to this stringer.
     *
     * @return this stringer.
     */
    public JSONWriter value(boolean value) throws JSONException, IOException {
        if (stack.isEmpty()) {
            throw new JSONException("Nesting problem");
        }
        beforeValue();
        append(Boolean.toString(value));
        return this;
    }

    /**
     * Encodes {@code value} to this stringer.
     *
     * @param value a finite value. May not be {@link Double#isNaN() NaNs} or
     *     {@link Double#isInfinite() infinities}.
     * @return this stringer.
     */
    public JSONWriter value(double value) throws JSONException, IOException {
        if (stack.isEmpty()) {
            throw new JSONException("Nesting problem");
        }
        beforeValue();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new JSONException("Cannot encode non-number doubles");
        }
        append(Double.toString(value));
        return this;
    }

    /**
     * Encodes {@code value} to this stringer.
     *
     * @param value a finite value. May not be {@link Double#isNaN() NaNs} or
     *     {@link Double#isInfinite() infinities}.
     * @return this stringer.
     */
    public JSONWriter value(float value) throws JSONException, IOException {
        if (stack.isEmpty()) {
            throw new JSONException("Nesting problem");
        }
        beforeValue();
        if (Float.isNaN(value) || Float.isInfinite(value)) {
            throw new JSONException("Cannot encode non-number doubles");
        }
        append(Float.toString(value));
        return this;
    }

    /**
     * Encodes {@code value} to this stringer.
     *
     * @return this stringer.
     */
    public JSONWriter value(int value) throws JSONException, IOException {
        if (stack.isEmpty()) {
            throw new JSONException("Nesting problem");
        }
        beforeValue();
        append(Integer.toString(value));
        return this;
    }

    /**
     * Encodes {@code value} to this stringer.
     *
     * @return this stringer.
     */
    public JSONWriter value(long value) throws JSONException, IOException {
        if (stack.isEmpty()) {
            throw new JSONException("Nesting problem");
        }
        beforeValue();
        append(Long.toString(value));
        return this;
    }

    private void string(String value) throws IOException {
        isEmpty = false;
        out.append('"');
        for (int i = 0, length = value.length(); i < length; i++) {
            char c = value.charAt(i);

            /*
             * From RFC 4627, "All Unicode characters may be placed within the
             * quotation marks except for the characters that must be escaped:
             * quotation mark, reverse solidus, and the control characters
             * (U+0000 through U+001F)."
             */
            switch (c) {
                case '"':
                case '\\':
                case '/':
                    out.append('\\').append(c);
                    break;

                case '\t':
                    out.append("\\t");
                    break;

                case '\b':
                    out.append("\\b");
                    break;

                case '\n':
                    out.append("\\n");
                    break;

                case '\r':
                    out.append("\\r");
                    break;

                case '\f':
                    out.append("\\f");
                    break;

                default:
                    if (c <= 0x1F) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                    break;
            }

        }
        out.append("\"");
    }

    private void newline() throws IOException {
        if (indent == null) {
            return;
        }

        isEmpty = false;
        out.append('\n');
        for (int i = 0; i < stack.size(); i++) {
            out.append(indent);
        }
    }

    /**
     * Encodes the key (property name) to this stringer.
     *
     * @param name the name of the forthcoming value. May not be null.
     * @return this stringer.
     */
    public JSONWriter key(String name) throws JSONException, IOException {
        if (name == null) {
            throw new JSONException("Names must be non-null");
        }
        beforeKey();
        string(name);
        return this;
    }

    /**
     * Inserts any necessary separators and whitespace before a name. Also
     * adjusts the stack to expect the key's value.
     */
    private void beforeKey() throws JSONException, IOException {
        Scope context = peek();
        if (context == Scope.NONEMPTY_OBJECT) { // first in object
            append(',');
        } else if (context != Scope.EMPTY_OBJECT) { // not in an object!
            throw new JSONException("Nesting problem");
        }
        newline();
        replaceTop(Scope.DANGLING_KEY);
    }

    /**
     * Inserts any necessary separators and whitespace before a literal value,
     * inline array, or inline object. Also adjusts the stack to expect either a
     * closing bracket or another element.
     */
    private void beforeValue() throws JSONException, IOException {
        if (stack.isEmpty()) {
            return;
        }

        Scope context = peek();
        if (context == Scope.EMPTY_ARRAY) { // first in array
            replaceTop(Scope.NONEMPTY_ARRAY);
            newline();
        } else if (context == Scope.NONEMPTY_ARRAY) { // another in array
            append(',');
            newline();
        } else if (context == Scope.DANGLING_KEY) { // value for key
            append(indent == null ? ":" : ": ");
            replaceTop(Scope.NONEMPTY_OBJECT);
        } else if (context != Scope.NULL) {
            throw new JSONException("Nesting problem");
        }
    }

    /**
     * Returns the encoded JSON string.
     *
     * <p>If invoked with unterminated arrays or unclosed objects, this method's
     * return value is undefined.
     *
     * <p><strong>Warning:</strong> although it contradicts the general contract
     * of {@link Object#toString}, this method returns null if the stringer
     * contains no data.
     */
    @Override
    public String toString() {
        return "JSONWriter";
    }
}
