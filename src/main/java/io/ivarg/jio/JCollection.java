package io.ivarg.jio;


import java.lang.reflect.ParameterizedType;
import java.util.Objects;
import java.util.logging.Logger;
import net.jodah.typetools.TypeResolver;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class JCollection<T> {
  private static Logger log = Logger.getLogger(JCollection.class.getName());

  protected Class<T> pcolClass;

  protected JioContext context;

  public PCollection<T> pcol;

  public JCollection(PCollection<T> pcol, JioContext context) {
    this.pcol = pcol;
    this.context = context;
    this.pcolClass = (Class<T>) pcol.getTypeDescriptor().getRawType();
    log.info(String.format("coder: %s", pcol.getCoder()));
  }

  public JCollection<KV<T, Long>> countByValue() {
    return context.newCollection(pcol.apply(Count.perElement()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <OutputT> JCollection<OutputT> map(
      SerializableFunction<T, OutputT> lmbd, Class<?> innerType) {
    var typeArgs = TypeResolver.resolveRawArguments(SerializableFunction.class, lmbd.getClass());
    final MapElements<T, OutputT> f =
        MapElements.into(TypeDescriptor.of(typeArgs[1])).via((ProcessFunction) lmbd);
    var pout = pcol.apply(pcol.getName(), f);
    log.info(pout.getCoder().toString());
    return context.newCollection(pout);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <OutputT> JCollection<OutputT> map(String name, SerializableFunction<T, OutputT> lmbd) {
    var typeArgs = TypeResolver.resolveRawArguments(SerializableFunction.class, lmbd.getClass());
    final MapElements<T, OutputT> f =
        MapElements.into(TypeDescriptor.of(typeArgs[1])).via((ProcessFunction) lmbd);
    var pout = pcol.apply(name, f);
    log.info(pout.getCoder().toString());
    return context.newCollection(pout);
  }

  public <OutputT> JCollection<OutputT> map(SerializableFunction<T, OutputT> lmbd) {
    return map(pcol.getName(), lmbd);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <OutputT> JCollection<OutputT> flatMap(String name, SerializableFunction<T, OutputT> fn) {

    var typeArgs = TypeResolver.resolveRawArguments(SerializableFunction.class, fn.getClass());
    var td = TypeDescriptor.of(typeArgs[1]);
    var tt = TypeDescriptors.iterables(TypeDescriptors.strings());

    FlatMapElements<T, OutputT> f = FlatMapElements.into(tt).via((ProcessFunction) fn);

    var pout = pcol.apply(name, f);
    log.info(pout.getCoder().toString());
    return context.newCollection(pout);
  }

  public <OutputT> JCollection<OutputT> flatMapIterable(
      String name, SerializableFunction<T, Iterable<OutputT>> fn) {

    try {
      var typeArgs = TypeResolver.resolveRawArguments(SerializableFunction.class, fn.getClass());

      var reified =
          (ParameterizedType) TypeResolver.reify(SerializableFunction.class, fn.getClass());

      var outputT =
          ((ParameterizedType) reified.getActualTypeArguments()[1]).getActualTypeArguments()[0];
      var tt = TypeDescriptor.of(outputT);

      FlatMapElements<T, OutputT> fmap = FlatMapElements.into(tt).via((ProcessFunction) fn);

      var pout = pcol.apply(name, fmap);
      log.info(pout.getCoder().toString());
      return context.newCollection(pout);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public <OutputT> JCollection<OutputT> flatMap(SerializableFunction<T, OutputT> lmbd) {
    return flatMap(pcol.getName(), lmbd);
  }

  /*
  public <OutputT> JCollection<OutputT> flatMapIterable(
      SerializableFunction<T, Iterable<OutputT>> fn) {
    return flatMapIterable(pcol.getName(), fn);
  }

   */

  public <OutputT> JCollection<OutputT> map2(SerializableFunction<T, OutputT> lmbd) {
    var typeArgs = TypeResolver.resolveRawArguments(SerializableFunction.class, lmbd.getClass());
    var td = (TypeDescriptor<OutputT>) TypeDescriptor.of(typeArgs[1]);
    log.info(String.format("map2, output type descriptor: %s", td));
    PTransform<PCollection<? extends T>, PCollection<OutputT>> transform =
        ParDo.of(
            new DoFn<T, OutputT>() {
              @ProcessElement
              public void processElement(ProcessContext ctxt) {
                var out = lmbd.apply(ctxt.element());
                ctxt.output(out);
              }
            });
    var pout = pcol.apply(transform).setTypeDescriptor(td);
    log.info(pout.getCoder().toString());
    return context.newCollection(pout);
  }

  public JCollection<Long> count() {
    return context.newCollection(pcol.apply(Count.globally()));
  }

  public void saveAsTextFile(String path) {
    saveAsTextFileFileIO(path);
    //    saveAsTextFileTextIO(path);
  }

  public void saveAsAvroFile(String path) {
    log.info(String.format("saveAsAvroFile path: {}", path));
    pcol.apply(
        FileIO.<T>write()
            .via(AvroIO.sink(pcolClass))
            .to(path)
            .withNaming(FileIO.Write.defaultNaming("part", ".avro")));
  }

  private void saveAsTextFileFileIO(String path) {
    pcol.apply(
        FileIO.<T>write()
            .via((FileIO.Sink<T>) TextIO.sink())
            .to(path)
            .withNaming(FileIO.Write.defaultNaming("output", ".txt")));
  }

  private void saveAsTextFileTextIO(String path) {
    var td = pcol.getTypeDescriptor();
    final PCollection<String> ss;
    if (td != null && td.getType().equals(String.class)) {
      ss = castPcol();
    } else {
      ss = pcol.apply(MapElements.into(TypeDescriptors.strings()).via(Objects::toString));
    }
    ss.apply(TextIO.write().to(path));
  }

  @SuppressWarnings("unchecked")
  private <NewT> PCollection<NewT> castPcol() {
    return (PCollection<NewT>) pcol;
  }
}
