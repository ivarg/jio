package io.ivarg.jio.testing;

import io.ivarg.jio.JCollection;
import io.ivarg.jio.JioContext;
import java.util.logging.Logger;
import org.apache.beam.sdk.values.PCollection;

public class TestJCollection<T> extends JCollection<T> {
  private static Logger log = Logger.getLogger(TestJCollection.class.getName());

  public TestJCollection(final PCollection<T> pcol, JioContext context) {
    super(pcol, context);
  }

  @Override
  public void saveAsAvroFile(String path) {
    log.info(String.format("Recording output %s", path));
    TestJioContext ctx = (TestJioContext) context;
    ctx.addOutput(path, pcol);
  }
}
