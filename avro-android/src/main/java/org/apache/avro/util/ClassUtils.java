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
package org.apache.avro.util;

public class ClassUtils {

  private ClassUtils() {
  }

  public static Class<?> forNestedName(ClassLoader classLoader, String className) {
    try {
      return ClassUtils.forName(classLoader, className);
    } catch (ClassNotFoundException e) {
      // This might be a nested namespace. Try using the last tokens in the
      // namespace as an enclosing class by progressively replacing period
      // delimiters with $
      StringBuilder nestedName = new StringBuilder(className);
      int lastDot = className.lastIndexOf('.');
      while (lastDot != -1) {
        nestedName.setCharAt(lastDot, '$');
        try {
          return ClassUtils.forName(classLoader, nestedName.toString());
        } catch (ClassNotFoundException ignored) {
        }
        lastDot = className.lastIndexOf('.', lastDot - 1);
      }
      return null;
    }
  }

  /**
   * Loads a class using the class loader. 1. The class loader of the context
   * class is being used. 2. The thread context class loader is being used. If
   * both approaches fail, returns null.
   *
   * @param classLoader The classloader to use.
   * @param className   The name of the class to load
   * @return The class or null if no class loader could load the class.
   */
  public static Class<?> forName(ClassLoader classLoader, String className) throws ClassNotFoundException {
    Class<?> c = null;
    if (classLoader != null) {
      c = forName(className, classLoader);
    }
    if (c == null && Thread.currentThread().getContextClassLoader() != null) {
      c = forName(className, Thread.currentThread().getContextClassLoader());
    }
    if (c == null) {
      throw new ClassNotFoundException("Failed to load class" + className);
    }
    return c;
  }

  /**
   * Loads a {@link Class} from the specified {@link ClassLoader} without throwing
   * {@link ClassNotFoundException}.
   *
   * @param className
   * @param classLoader
   * @return
   */
  private static Class<?> forName(String className, ClassLoader classLoader) {
    Class<?> c = null;
    if (classLoader != null && className != null) {
      try {
        c = Class.forName(className, true, classLoader);
      } catch (ClassNotFoundException e) {
        // Ignore and return null
      }
    }
    return c;
  }
}
