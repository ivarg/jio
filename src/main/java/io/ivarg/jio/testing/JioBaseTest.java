package io.ivarg.jio.testing;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;

public abstract class JioBaseTest {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  protected JobTest createJobTest(Class<?> jobClass) {
    return JobTest.create(jobClass, testPipeline);
  }
}
