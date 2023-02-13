package io.ivarg.jio;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.beam.sdk.transforms.SerializableFunctions.identity;

import com.spotify.scio.parquet.read.ParquetRead;
import com.spotify.scio.parquet.read.ReadSupportFactory;
import io.ivarg.jio.coder.AvroToJio;
import io.ivarg.jio.coder.JioCoder;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;

public class JioContext {
  private static Logger log = Logger.getLogger(JioContext.class.getName());

  protected final UserArgs userArgs;
  protected final Pipeline pipeline;

  protected JioContext(Pipeline pipeline, UserArgs userArgs) {
    this.pipeline = pipeline;
    this.userArgs = userArgs;
  }

  /** Accept only args on the form --<arg name>=<arg value>. */
  public static JioContext fromArgs(String[] cmdLineArgs) {
    Pattern p = Pattern.compile("--([^=]+).*");

    // filter and map provided options
    Map<String, String> providedOptionsMap =
        Stream.of(cmdLineArgs)
            .flatMap(arg -> p.matcher(arg).results())
            .map(m -> Map.entry(m.group(1), m.group(0)))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    // bail if there are non-valid args
    validateArgs(cmdLineArgs, providedOptionsMap);

    Set<String> pipelineOptionNames = pipelineOptionNames();
    pipelineOptionNames.retainAll(providedOptionsMap.keySet());

    PipelineOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(
                getPipelineOptions(providedOptionsMap, pipelineOptionNames).toArray(new String[0]))
            .withValidation()
            .create();

    UserArgs userArgs =
        new UserArgs(
            getUserOptions(providedOptionsMap, pipelineOptionNames).stream()
                .map(o -> o.substring(2).split("="))
                .map(
                    pair -> {
                      var value = "false";
                      if (pair.length == 2) {
                        value = pair[1];
                      }
                      return Map.entry(pair[0], value);
                    })
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));

    return new JioContext(Pipeline.create(pipelineOptions), userArgs);
  }

  public UserArgs userArgs() {
    return userArgs;
  }

  public <T> JCollection<T> newCollection(PCollection<T> pcol) {
    return new JCollection<>(pcol, this);
  }

  public <T> JCollection<T> typedAvroFile(Class<T> recordClass, String path) {
    PCollection<T> pcol =
        pipeline.apply(AvroIO.read(recordClass).from(path)).setCoder(AvroCoder.of(recordClass));

    return newCollection(pcol);
  }

  // Here we call the Scio function `ParquetRead.read()` function to create a transform.
  // Perhaps it can be done by going directly to beam?
  public <T> JCollection<T> typedParquetFile1(Class<T> recordClass, String path) {
    Coder<T> coder = AvroCoder.of(recordClass);
    ReadSupportFactory<T> readSupportFactory = ReadSupportFactory.avro();
    Configuration conf = new Configuration(false);
    SerializableConfiguration sconf = new SerializableConfiguration(conf);
    SerializableFunction<T, T> ident = identity();
    PTransform<PBegin, PCollection<T>> ptrans =
        ParquetRead.read(readSupportFactory, sconf, path, ident);
    return newCollection(pipeline.apply(ptrans).setCoder(coder));
  }

  public <T> JCollection<T> typedParquetFile2(Class<T> recordClass, String path) {
    Schema schema = ReflectData.get().getSchema(recordClass);
    Coder<T> coder = AvroCoder.of(recordClass);
    //    PTransform<PBegin, PCollection<T>> read = ParquetIO.read(schema);
    ParquetIO.Read read = ParquetIO.read(schema).from(path);
    var out = (PCollection<T>) pipeline.apply(read);
    return newCollection(out);
  }

  public <T> JCollection<T> typedParquetFile(Class<T> recordClass, String path) {
    Schema schema = ReflectData.get().getSchema(recordClass);
    //    Coder<T> coder = AvroCoder.of(recordClass);
    Coder<T> coder = JioCoder.of(recordClass);

    var tf =
        ParquetIO.parseGenericRecords(
                (SerializableFunction<GenericRecord, T>)
                    input -> {
                      try {

                        // Using `parseGenericRecords` we get the full parquet rows (all columns)
                        // represented by a GenericRecord. I haven't found any pre-built way to
                        // convert that record to a SpecificRecord (our own provided class), which
                        // is strange. But we can always resort to manually build a POJO with the
                        // help of the schemas.
                        // The conversion suggested here:
                        // https://stackoverflow.com/a/59442020/919665
                        // didn't work
                        //                    T specData = (T) SpecificData.get().deepCopy(schema,
                        // input);
                        //
                        //
                        // Next level is to read the Parquet directly and more manually, like they
                        // do
                        // in Scio, and provide a DoFn that reads only the columns we need, etc

                        T output = AvroToJio.convert(input, recordClass);
                        return output;
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    })
            .from(path)
            .withCoder(coder);
    return newCollection(pipeline.apply(tf).setCoder(coder));
  }

  // This is inspired by the `readLegacy()` method in the ParquetTypeIO class in scio-parquet,
  // which uses HadoopFormatIO to read. Alas, I didn't get it to work. Leaving it here for
  // documentation and log
  public <T> JCollection<T> typedParquetFile4(Class<T> recordClass, String path) {
    log.info(path);
    Schema schema = ReflectData.get().getSchema(recordClass);
    Coder<T> coder = AvroCoder.of(recordClass);

    Configuration hconf = new Configuration(false);
    // Set Hadoop InputFormat, key and value class in configuration
    hconf.setClass("mapreduce.job.inputformat.class", ParquetInputFormat.class, InputFormat.class);
    hconf.setClass("key.class", Void.class, Void.class);
    hconf.setClass("value.class", recordClass, recordClass);

    PCollection<T> pcol =
        pipeline
            .apply(
                "read",
                HadoopFormatIO.<Boolean, T>read()
                    .withKeyTranslation(
                        SimpleFunction.fromSerializableFunctionWithOutputType(
                            (SerializableFunction<Void, Boolean>) input -> true,
                            TypeDescriptors.booleans()))
                    .withValueTranslation(
                        SimpleFunction.fromSerializableFunctionWithOutputType(
                            (SerializableFunction<T, T>) input -> input,
                            TypeDescriptor.of(recordClass)))
                    .withConfiguration(hconf)
                    .withSkipKeyClone(true))
            .apply(Values.create());

    return newCollection(pcol);
  }

  public void runAndWait() {
    var result = pipeline.run();
    result.waitUntilFinish();
  }

  private static Set<String> getUserOptions(
      Map<String, String> providedOptionsMap, Set<String> pipelineOptionNames) {
    return providedOptionsMap.entrySet().stream()
        .filter(entry -> !pipelineOptionNames.contains(entry.getKey()))
        .map(Map.Entry::getValue)
        .collect(toSet());
  }

  private static Set<String> getPipelineOptions(
      Map<String, String> providedOptionsMap, Set<String> pipelineOptionNames) {
    return providedOptionsMap.entrySet().stream()
        .filter(entry -> pipelineOptionNames.contains(entry.getKey()))
        .map(Map.Entry::getValue)
        .collect(toSet());
  }

  private static void validateArgs(String[] args, Map<String, String> parsedArgs) {
    var badArgs = Stream.of(args).filter(a -> !parsedArgs.containsValue(a)).collect(toSet());
    if (badArgs.size() > 0) {
      log.info(String.format("unrecognized options: %s", badArgs));
      //      throw new RuntimeException("the following args are not valid: " + badArgs);
    }
  }

  // extract the names of all registered pipeline options
  private static Set<String> pipelineOptionNames() {
    return PipelineOptionsFactory.getRegisteredOptions().stream()
        .flatMap(c -> Stream.of(c.getMethods()))
        .distinct()
        .filter(
            m ->
                (m.getName().startsWith("get") || m.getName().startsWith("is"))
                    && m.getParameterCount() < 1
                    && m.getReturnType().getModule().getName() != null
                    && m.getReturnType().getModule().getName().equals("java.base"))
        .map(Method::getName)
        .map(
            name -> {
              if (name.startsWith("get")) {
                return UPPER_CAMEL.to(LOWER_CAMEL, name.substring(3));
              } else if (name.startsWith("is")) {
                return UPPER_CAMEL.to(LOWER_CAMEL, name.substring(2));
              } else {
                throw new RuntimeException("unexpected pipeline option");
              }
            })
        .collect(toSet());
  }
}
