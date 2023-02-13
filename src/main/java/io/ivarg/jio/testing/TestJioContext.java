package io.ivarg.jio.testing;

import static java.util.stream.Collectors.toMap;

import io.ivarg.jio.JCollection;
import io.ivarg.jio.JioContext;
import io.ivarg.jio.UserArgs;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

public class TestJioContext extends JioContext {

  private Map<String, Collection<?>> inputs = new HashMap<>();
  private Map<String, PCollection<?>> outputs = new HashMap<>();

  private Map<String, Consumer<JioAsserter>> asserts = new HashMap<>();

  private Map<String, String> args = new HashMap<>();

  public TestJioContext(UserArgs userArgs, Pipeline pipeline) {
    super(pipeline, userArgs);
  }

  // TODO: This is somewhat duplicated from JioContext.fromArgs()
  public static TestJioContext fromArgs(String[] args, Pipeline pipeline) {
    Pattern p = Pattern.compile("--([^=]+).*");

    // filter and map provided options
    Map<String, String> providedOptionsMap =
        Stream.of(args)
            .flatMap(arg -> p.matcher(arg).results())
            .map(m -> Map.entry(m.group(1), m.group(0)))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    UserArgs userArgs =
        new UserArgs(
            providedOptionsMap.values().stream()
                //            getUserOptions(providedOptionsMap, pipelineOptionNames).stream()
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

    return new TestJioContext(userArgs, pipeline);
  }

  public <T> void addInput(String path, Collection<T> items) {
    args.put(path, path);
    inputs.put(path, items);
  }

  public <T> void addOutput(String path, PCollection<T> pcol) {
    outputs.put(path, pcol);
  }

  public void addAssert(String key, Consumer<JioAsserter> assertFn) {
    asserts.put(key, assertFn);
  }

  public Map<String, PCollection<?>> outputs() {
    return outputs;
  }

  @Override
  public <T> JCollection<T> newCollection(PCollection<T> pcol) {
    return new TestJCollection<>(pcol, this);
  }

  @Override
  public <T> JCollection<T> typedAvroFile(final Class<T> recordClass, final String path) {
    Collection<T> items = (Collection<T>) inputs.get(path);
    Create.Values<T> values = Create.of(items);
    return new TestJCollection<>(pipeline.apply(values), this);
  }

  @Override
  public void runAndWait() {
    asserts.forEach(
        (key, fn) -> {
          var pcol = outputs.get(key);
          JioAsserter<?> asserter = new JioAsserter<>(pcol);
          fn.accept(asserter);
        });
    pipeline.run();
  }
}
