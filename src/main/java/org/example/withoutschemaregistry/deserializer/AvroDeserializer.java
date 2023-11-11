package org.example.withoutschemaregistry.deserializer;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.avro.SimpleMessage;

import java.util.Map;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) {
        // do nothing
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        T returnObject = null;

        try {

            if (bytes != null) {
                DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(SimpleMessage.class.newInstance().getSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                returnObject = (T) datumReader.read(null, decoder);
            }
        } catch (Exception e) {
            System.out.println("Unable to Deserialize bytes[] " + e);
        }

        return returnObject;
    }

    @Override
    public void close() {
        // do nothing
    }
}