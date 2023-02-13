package io.ivarg.jio.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

public class JioCoder<T> extends CustomCoder<T> {

  private final Class<T> cls;

  private final AvroCoder<GenericRecord> avroCoder;

  public static <T> JioCoder<T> of(Class<T> type) {
    Schema schema = JioToAvro.generateSchema(type);
    return new JioCoder<>(type, schema);
  }

  protected JioCoder(Class<T> cls, Schema schema) {
    this.cls = cls;
    avroCoder = AvroCoder.of(GenericRecord.class, schema);
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    GenericRecord record = JioToAvro.convert(value);
    avroCoder.encode(record, outStream);
  }

  @Override
  public T decode(final InputStream inStream) throws CoderException, IOException {
    GenericRecord record = avroCoder.decode(inStream);
    try {
      return AvroToJio.convert(record, cls);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
