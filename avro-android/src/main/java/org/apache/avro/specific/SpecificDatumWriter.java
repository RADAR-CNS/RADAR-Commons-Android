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
package org.apache.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

import java.io.IOException;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for generated Java
 * classes.
 */
public class SpecificDatumWriter<T> extends GenericDatumWriter<T> {

  public SpecificDatumWriter(Schema schema) {
    super(schema, SpecificData.get());
  }

  public SpecificDatumWriter(Schema root, SpecificData specificData) {
    super(root, specificData);
  }

  /** Returns the {@link SpecificData} implementation used by this writer. */
  public SpecificData getSpecificData() {
    return (SpecificData) getData();
  }

  @Override
  protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
    if (!(datum instanceof Enum))
      super.writeEnum(schema, datum, out); // punt to generic
    else
      out.writeEnum(((Enum) datum).ordinal());
  }

  @Override
  protected void writeField(Object datum, Schema.Field f, Encoder out) throws IOException {
    if (datum instanceof SpecificRecordBase) {
      Schema fieldSchema = f.schema();
      Object value = getData().getField(datum, f.name(), f.pos());
      writeWithoutConversion(fieldSchema, value, out);
    } else {
      super.writeField(datum, f, out);
    }
  }
}
