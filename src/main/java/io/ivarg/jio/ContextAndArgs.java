package io.ivarg.jio;

import io.ivarg.jio.testing.TestJioContext;
import org.apache.beam.sdk.Pipeline;

public abstract class ContextAndArgs {

  private static TestJioContext testContext = null;

  public static JioContext parse(String[] cmdLineArgs) {
    if (testContext != null) {
      return testContext;
    } else {
      return JioContext.fromArgs(cmdLineArgs);
    }
  }

  public static TestJioContext setupTest(String[] args, Pipeline pipeline) {
    testContext = TestJioContext.fromArgs(args, pipeline);
    return testContext;
  }
}
