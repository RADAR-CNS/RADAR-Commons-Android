package org.apache.avro.specific;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Before;
import org.junit.Test;
import org.radarbase.android.data.serialization.TapeAvroDeserializer;
import org.radarbase.android.data.serialization.TapeAvroSerializer;
import org.radarbase.data.AvroDatumDecoder;
import org.radarbase.data.AvroDecoder;
import org.radarbase.data.AvroEncoder;
import org.radarbase.data.Record;
import org.radarbase.data.RemoteSchemaEncoder;
import org.radarbase.producer.rest.ParsedSchemaMetadata;
import org.radarbase.topic.AvroTopic;
import org.radarcns.active.questionnaire.Answer;
import org.radarcns.active.questionnaire.Questionnaire;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneAcceleration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SpecificRecordTest {
    private AvroTopic<ObservationKey, PhoneAcceleration> topic;
    private Record<ObservationKey, PhoneAcceleration> record;

    @Before
    public void setUpTopic() {
        topic = new AvroTopic<>("test",
                ObservationKey.getClassSchema(), PhoneAcceleration.getClassSchema(),
                ObservationKey.class, PhoneAcceleration.class);
        record = new Record<>(
                new ObservationKey("p", "u", "s"),
                new PhoneAcceleration(0.0d, 0.1d, 0.2f, 0.3f, 0.4f));
    }

    @Test
    public void serializationTest() throws IOException {
        TapeAvroSerializer<ObservationKey, PhoneAcceleration> avroSerializer = new TapeAvroSerializer<>(topic, SpecificData.get());
        TapeAvroDeserializer<ObservationKey, PhoneAcceleration> avroDeserializer = new TapeAvroDeserializer<>(topic, SpecificData.get());

        byte[] result;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            avroSerializer.serialize(record, out);
            result = out.toByteArray();
        }

        Record<ObservationKey, PhoneAcceleration> actual;
        try (ByteArrayInputStream input = new ByteArrayInputStream(result)) {
            actual = avroDeserializer.deserialize(input);
        }

        assertEquals(record.key, actual.key);
        assertEquals(record.value, actual.value);
    }

    @Test
    public void avroBinaryEncodingTest() throws IOException, SchemaValidationException {
        AvroEncoder encoder = new RemoteSchemaEncoder(true);
        AvroEncoder.AvroWriter<PhoneAcceleration> accelerationWriter = encoder.writer(PhoneAcceleration.getClassSchema(), PhoneAcceleration.class);
        ParsedSchemaMetadata schemaMetadata = new ParsedSchemaMetadata(1, 1, new Schema.Parser().parse("{\"name\":\"Test\",\"type\":\"record\",\"fields\":[{\"name\":\"time\",\"type\":\"double\"},{\"name\":\"timeReceived\",\"type\":\"double\"},{\"name\":\"x\",\"type\":\"float\"},{\"name\":\"y\",\"type\":\"float\"},{\"name\":\"z\",\"type\":\"float\"},{\"name\":\"def\",\"type\":[\"null\",\"string\"],\"default\":null}]}"));
        accelerationWriter.setReaderSchema(schemaMetadata);
        byte[] result = accelerationWriter.encode(record.value);

        AvroDecoder decoder = new AvroDatumDecoder(SpecificData.get(), true);
        AvroDecoder.AvroReader<PhoneAcceleration> accelerationReader = decoder.reader(PhoneAcceleration.getClassSchema(), PhoneAcceleration.class);
        GenericRecord acceleration = accelerationReader.decode(result);
        assertEquals(record.value, acceleration);
    }


    @Test
    public void avroJsonEncodingTest() throws IOException, SchemaValidationException {
        AvroEncoder encoder = new RemoteSchemaEncoder(false);
        AvroEncoder.AvroWriter<GenericRecord> accelerationWriter = encoder.writer(Questionnaire.getClassSchema(), GenericRecord.class);
        ParsedSchemaMetadata schemaMetadata = new ParsedSchemaMetadata(1, 1, Questionnaire.getClassSchema());
        accelerationWriter.setReaderSchema(schemaMetadata);
        List<Answer> list = new ArrayList<>(2);
        list.add(new Answer("qid1", 1, 0.4, 0.5));
        list.add(new Answer("qid2", "a", 0.6, 0.7));
        byte[] result = accelerationWriter.encode(new Questionnaire(0.1, 0.2, 0.3, "test", "2", list));

        System.out.println("Result: " + new String(result) + " (len " + result.length + ")");

        AvroDecoder decoder = new AvroDatumDecoder(SpecificData.get(), false);
        AvroDecoder.AvroReader<Questionnaire> accelerationReader = decoder.reader(Questionnaire.getClassSchema(), Questionnaire.class);
        GenericRecord acceleration = accelerationReader.decode(result);
        assertEquals(new Questionnaire(0.1, 0.2, 0.3, "test", "2", list), acceleration);
    }


    @Test
    public void avroMiscEncodingTest() throws IOException, SchemaValidationException {
        AvroEncoder encoder = new RemoteSchemaEncoder(false);

        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"bytes\",\"type\":\"bytes\"},{\"name\":\"bool\",\"type\":\"boolean\"},{\"name\":\"unionFixed\",\"type\":[\"null\",{\"type\":\"fixed\",\"size\":4,\"name\":\"four\"}],\"default\":null},{\"name\":\"map\",\"type\":{\"type\":\"map\", \"values\":{\"type\":\"int\"}}}]}");
        AvroEncoder.AvroWriter<GenericRecord> accelerationWriter = encoder.writer(schema, GenericRecord.class);
        ParsedSchemaMetadata schemaMetadata = new ParsedSchemaMetadata(1, 1, schema);
        accelerationWriter.setReaderSchema(schemaMetadata);
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        record.set("bytes", ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
        Map<String, Integer> map = new HashMap<>();
        map.put("m1", 5);
        map.put("m2", 6);
        record.set("bool", false);
        record.set("map", map);
        record.set("unionFixed", new GenericData.Fixed(schema.getField("unionFixed").schema().getTypes().get(1), new byte[] {8, 9, 10, 11}));
        byte[] encoded = accelerationWriter.encode(record.build());

        System.out.println("Result: " + new String(encoded) + " (len " + encoded.length + ")");

        AvroDecoder decoder = new AvroDatumDecoder(SpecificData.get(), false);
        AvroDecoder.AvroReader<GenericRecord> reader = decoder.reader(schema, GenericRecord.class);
        GenericRecord result = reader.decode(encoded);
        assertEquals(record.build(), result);
    }
}
