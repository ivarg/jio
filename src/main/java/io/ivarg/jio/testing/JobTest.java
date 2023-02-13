package io.ivarg.jio.testing;

import io.ivarg.jio.ContextAndArgs;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.function.Consumer;
import org.apache.beam.sdk.Pipeline;

public class JobTest {

  private Pipeline pipeline;
  private TestJioContext context;
  private Class<?> jobClass;

  private JobTest(Class<?> jobClass, Pipeline pipeline) {
    this.jobClass = jobClass;
    this.pipeline = pipeline;
  }

  public static JobTest create(Class<?> jobClass, Pipeline pipeline) {
    return new JobTest(jobClass, pipeline);
  }

  public JobTest args(String... args) {
    context = ContextAndArgs.setupTest(args, pipeline);
    return this;
  }

  public <T> JobTest input(String argName, Collection<T> items) {
    context.addInput(argName, items);
    return this;
  }

  public JobTest output(String key, Consumer<JioAsserter> assertFn) {
    context.addAssert(key, assertFn);
    return this;
  }

  public void run() {
    try {
      var main = jobClass.getMethod("main", String[].class);
      String[] params = new String[] {};
      main.invoke(null, (Object) params);
    } catch (InvocationTargetException e) {
      var target = e.getTargetException();
      if (target instanceof AssertionError) {
        if (target.getCause() != null) {
          target = target.getCause();
        }
        throw (AssertionError) target;
      }
      throw (RuntimeException) target;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
