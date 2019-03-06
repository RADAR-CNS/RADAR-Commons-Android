/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.android.data

import org.apache.avro.generic.GenericData
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import org.radarcns.data.Record
import org.radarcns.topic.AvroTopic
import org.radarcns.util.BackedObjectQueue
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream

/**
 * Converts records from an AvroTopic for Tape
 */
class TapeAvroSerializer<K, V>(topic: AvroTopic<K, V>, specificData: GenericData) : BackedObjectQueue.Serializer<Record<K, V>> {
    private val encoderFactory: EncoderFactory = EncoderFactory.get()
    @Suppress("UNCHECKED_CAST")
    private val keyWriter: DatumWriter<K> = specificData.createDatumWriter(topic.keySchema) as DatumWriter<K>
    @Suppress("UNCHECKED_CAST")
    private val valueWriter: DatumWriter<V> = specificData.createDatumWriter(topic.valueSchema) as DatumWriter<V>
    private var encoder: BinaryEncoder? = null
    private var previousKey: K? = null
    private var keyBytes: ByteArray? = ByteArray(0)

    @Throws(IOException::class)
    override fun serialize(o: Record<K, V>, out: OutputStream) {
        // for backwards compatibility
        out.write(EMPTY_HEADER, 0, 8)

        out.write(if (o.key == previousKey) {
            keyBytes
        } else {
            val keyOut = ByteArrayOutputStream()
            encoder = encoderFactory.binaryEncoder(keyOut, encoder).also {
                keyWriter.write(o.key, it)
                it.flush()
            }
            previousKey = o.key
            keyOut.toByteArray().also { keyBytes = it }
        })

        encoder = encoderFactory.binaryEncoder(out, encoder).also {
            valueWriter.write(o.value, it)
            it.flush()
        }
    }

    companion object {
        private val EMPTY_HEADER = byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0)
    }
}
