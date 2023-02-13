package io.ivarg.jio.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StructuredCoder;

public class OptionalCoder<T extends Optional<U>, U> extends StructuredCoder<T> {

  /*
  public static <U> OptionalCoder of(TypeDescriptor<U> type) {
    @SuppressWarnings("unchecked")
    Class<U> clazz = (Class<U>) type.getRawType();
    return new OptionalCoder(valueCoder);
  }
  */

  public static <U> OptionalCoder of(Coder<U> valueCoder) {
    return new OptionalCoder(valueCoder);
  }

  private final NullableCoder<U> valueCoder;

  private OptionalCoder(Coder<U> valueCoder) {
    this.valueCoder = NullableCoder.of(valueCoder);
  }

  @Override
  public void encode(final T value, final OutputStream outStream)
      throws CoderException, IOException {
    if (value.isEmpty()) {
      valueCoder.encode(null, outStream);
    } else {
      valueCoder.encode(value.get(), outStream);
    }
  }

  @Override
  public T decode(final InputStream inStream) throws CoderException, IOException {
    U obj = valueCoder.decode(inStream);
    if (obj == null) {
      return (T) Optional.empty();
    }
    return (T) Optional.of(obj);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}

  /*
  static class OptionalCoderProvider extends CoderProvider {

    @Override
    public <T> Coder<T> coderFor(
        final TypeDescriptor<T> typeDescriptor, final List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      return null;

      if (Serializable.class.isAssignableFrom(typeDescriptor.getRawType())) {
        return SerializableCoder.of((TypeDescriptor) typeDescriptor);
      }
      throw new CannotProvideCoderException(
          "Cannot provide SerializableCoder because "
              + typeDescriptor
              + " does not implement Serializable");
    }
  }
   */
}
