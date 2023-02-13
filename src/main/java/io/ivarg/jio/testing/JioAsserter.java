package io.ivarg.jio.testing;

import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;

public class JioAsserter<T> {

  private PCollection<T> pcol;

  public JioAsserter(PCollection<T> pcol) {
    this.pcol = pcol;
  }

  public void shouldContainInAnyOrder(T... elems) {
    shouldContainInAnyOrder(List.of(elems));
  }

  public void shouldContainInAnyOrder(Iterable<T> iter) {
    PAssert.that(pcol).containsInAnyOrder(iter);
  }

  public void isEmpty() {
    PAssert.that(pcol).empty();
  }
}
