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

import org.json.JSONException;
import org.json.JSONObject;
import java.util.Iterator;

/**
 * Base class for objects that have JSON-valued properties. Avro and JSON values
 * are represented in Java using the following mapping:
 *
 * <table>
 * <th>
 * <td>Avro type</td>
 * <td>JSON type</td>
 * <td>Java type</td></th>
 * <tr>
 * <td><code>null</code></td>
 * <td><code>null</code></td>
 * <td>{@link #NULL_VALUE}</td>
 * </tr>
 * <tr>
 * <td><code>boolean</code></td>
 * <td>Boolean</td>
 * <td><code>boolean</code></td>
 * </tr>
 * <tr>
 * <td><code>int</code></td>
 * <td>Number</td>
 * <td><code>int</code></td>
 * </tr>
 * <tr>
 * <td><code>long</code></td>
 * <td>Number</td>
 * <td><code>long</code></td>
 * </tr>
 * <tr>
 * <td><code>float</code></td>
 * <td>Number</td>
 * <td><code>float</code></td>
 * </tr>
 * <tr>
 * <td><code>double</code></td>
 * <td>Number</td>
 * <td><code>double</code></td>
 * </tr>
 * <tr>
 * <td><code>bytes</code></td>
 * <td>String</td>
 * <td><code>byte[]</code></td>
 * </tr>
 * <tr>
 * <td><code>string</code></td>
 * <td>String</td>
 * <td>{@link java.lang.String}</td>
 * </tr>
 * <tr>
 * <td><code>record</code></td>
 * <td>Object</td>
 * <td>{@link java.util.Map}</td>
 * </tr>
 * <tr>
 * <td><code>enum</code></td>
 * <td>String</td>
 * <td>{@link java.lang.String}</td>
 * </tr>
 * <tr>
 * <td><code>array</code></td>
 * <td>Array</td>
 * <td>{@link java.util.Collection}</td>
 * </tr>
 * <tr>
 * <td><code>map</code></td>
 * <td>Object</td>
 * <td>{@link java.util.Map}</td>
 * </tr>
 * <tr>
 * <td><code>fixed</code></td>
 * <td>String</td>
 * <td><code>byte[]</code></td>
 * </tr>
 * </table>
 */
public abstract class JsonProperties {

  public static class Null {
    private Null() {
    }
  }

  /** A value representing a JSON <code>null</code>. */
  public static final Null NULL_VALUE = new Null();

  // use a ConcurrentHashMap for speed and thread safety, but keep a Queue of the
  // entries to maintain order
  // the queue is always updated after the main map and is thus is potentially a
  // subset of the map.
  // By making props private, we can control access and only implement/override
  // the methods
  // we need. We don't ever remove anything so we don't need to implement the
  // clear/remove functionality.
  // Also, we only ever ADD to the collection, never changing a value, so
  // putWithAbsent is the
  // only modifier
  private final JSONObject props = new JSONObject();

  JsonProperties() {
  }

  /**
   * Returns the value of the named, string-valued property in this schema.
   * Returns <tt>null</tt> if there is no string-valued property with that name.
   */
  public String getProp(String name) {
    String prop = props.optString(name);
    return !prop.equals("") ? prop : null;
  }

  public void addProp(String name, Object value) {
    try {
      props.put(name, value);
    } catch (JSONException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  public void putAll(JsonProperties np) {
    for (Iterator<String> it = np.props.keys(); it.hasNext(); ) {
      String key = it.next();
      try {
        props.put(key, np.props.get(key));
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    }
  }

  void writeProps(JSONObject gen) throws JSONException {
    for (Iterator<String> it = props.keys(); it.hasNext(); ) {
      String key = it.next();
      gen.put(key, props.get(key));
    }
  }

  int propsHashCode() {
    return props.hashCode();
  }

  boolean propsEqual(JsonProperties np) {
    return props.equals(np.props);
  }

  public boolean hasProps() {
    return props.length() > 0;
  }
}
