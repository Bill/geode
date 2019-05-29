package org.apache.geode.common.function.internal;

import java.util.function.Supplier;

public class Memoize {

  private static final Object UNSET = new Object();

  private Memoize() {}

  /**
   * Memoize a {@link Supplier}. Returns a new supplier based on the one provided. The first time
   * {@code get()} is called on the new supplier, it'll delegate to {@code delegate.get()} and
   * remember the result, and return it. Subsequent calls will simply return the remembered result.
   *
   * The returned {@link Supplier} is not safe for use from multiple threads.
   *
   * @param delegate is the original {@link Supplier}.
   * @param <T> the kind of object returned by {@link Supplier#get()}
   * @return a new {@link Supplier}. Calling {@code get()} on that supplier will always return the
   *         result of the first call to {@code delegate.get()}
   */
  @SuppressWarnings("unchecked")
  public static <T> Supplier<T> memoizeNotThreadSafe(final Supplier<T> delegate) {
    final Object[] cache = {UNSET};
    return () -> cache[0] == UNSET ? (T)(cache[0] = delegate.get()) : (T)cache[0];
  }
}
