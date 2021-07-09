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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.ResolvingDecoder;

import java.io.IOException;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for generated Java
 * classes.
 */
public class SpecificDatumReader<T> extends GenericDatumReader<T> {
  /** Construct where the writer's and reader's schemas are the same. */
  public SpecificDatumReader(Schema schema) {
    this(schema, schema, SpecificData.get());
  }

  /**
   * Construct given writer's schema, reader's schema, and a {@link SpecificData}.
   */
  public SpecificDatumReader(Schema writer, Schema reader, SpecificData data) {
    super(writer, reader, data);
  }

  /** Return the contained {@link SpecificData}. */
  public SpecificData getSpecificData() {
    return (SpecificData) getData();
  }

  @Override
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    return super.readRecord(old, expected, in);
  }

  @Override
  protected void readField(Object record, Schema.Field field, Object oldDatum, ResolvingDecoder in)
      throws IOException {
    if (record instanceof SpecificRecordBase) {
      Object datum;
      datum = readWithoutConversion(oldDatum, field.schema(), in);
      getData().setField(record, field.name(), field.pos(), datum);
    } else {
      super.readField(record, field, oldDatum, in);
    }
  }
}
