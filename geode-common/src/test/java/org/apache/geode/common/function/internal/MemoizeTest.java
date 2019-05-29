package org.apache.geode.common.function.internal;

import static org.apache.geode.common.function.internal.Memoize.memoizeNotThreadSafe;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

public class MemoizeTest {

  private int[] state;
  private Supplier<Integer> supplier;
  private Supplier<Integer> memoizedSupplier;

  @Before
  public void before() {
    state = new int[] {0};
    supplier = () -> state[0] = state[0] + 1;
    memoizedSupplier = memoizeNotThreadSafe(supplier);
  }

  @Test
  public void storageIsInitiallyZero() {
    assertThat(state[0]).as("initial state").isZero();
  }

  @Test
  public void firstGetReturnsComputedValue() {
    assertThat(memoizedSupplier.get()).as("first get() returns computed").isEqualTo(1);
  }

  @Test
  public void secondGetReturnsFirstComputedValue() {
    final Integer gotFirst = memoizedSupplier.get();
    final Integer gotSecond = memoizedSupplier.get();
    assertThat(gotSecond).as("second get() matches first").isEqualTo(gotFirst);
  }

  @Test
  public void secondGetDoesntModifyStorage() {
    final Integer gotFirst = memoizedSupplier.get();
    memoizedSupplier.get();
    assertThat(state[0]).as("final state").isEqualTo(gotFirst);
  }
}
