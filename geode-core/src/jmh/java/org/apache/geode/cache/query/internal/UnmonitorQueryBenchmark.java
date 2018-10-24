/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query.internal;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import org.apache.geode.internal.cache.InternalCache;

/**
 * The QueryMonitor tracks query execution time and cancels long-running ones.
 * A successful query un-registers itself from the query monitor.
 *
 * This class benchmarks un-registering a query from the QueryMonitor.
 *
 * Marginal performance of un-registering a query can depend on the number of queries
 * currently being monitored by the QueryMonitor.
 *
 * This benchmark varies the number of queries monitored, and at each level
 * it measures the marginal un-registering (time) cost.
 */
@State(Scope.Thread)
@Fork(1)
public class UnmonitorQueryBenchmark {
  private static long max_execution_time = 5;

  @Param({"1000", "10000", "100000"})
  public int initialNumberOfQueriesMonitored;

  /*
   * Depending on implementation, un-monitoring may exhibit different asymptotic
   * performance, for items submitted earlier vs. items submitted later
   */
  @Param({"true", "false"})
  public boolean removeYoungest; // if false, remove oldest

  private QueryMonitor monitor;
  private ArrayList<Thread> threads;
  private Thread thread;
  private DefaultQuery query;
  private InternalCache cache;

  @Setup(Level.Trial)
  public void trialSetup() {
    threads = createThreads(initialNumberOfQueriesMonitored);
    cache = mock(InternalCache.class);
  }

  @Setup(Level.Iteration)
  public void iterationSetup() {
    initializeMonitoredQueries();
    thread = threads.get(getThreadIndex());
    query = createDefaultQuery();
  }

  @Benchmark
  @Measurement(iterations = (int) 1e2)
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  // @Warmup we don't warm up because our @Setup warms us up
  public void unmonitorQuery() {
    monitor.stopMonitoringQueryThread(thread, query);
  }

  private DefaultQuery createDefaultQuery() {
    // we can reuse this because it doesn't affect lookup or equality in the collection(s)
    final DefaultQuery defaultQuery = mock(DefaultQuery.class);
    doReturn(new boolean[] {false}).when(defaultQuery).getQueryCompletedForMonitoring();
    return defaultQuery;
  }

  private ArrayList<Thread> createThreads(final int n) {
    final ArrayList<Thread> threads = new ArrayList<>(n);
    for (int i = 0; i < n; ++i) {
      threads.add(mock(Thread.class));
    }
    return threads;
  }

  private void initializeMonitoredQueries() {
    monitor = new QueryMonitor(cache, max_execution_time);

    for (Thread t : threads) {
      final DefaultQuery query = createDefaultQuery();
      monitor.monitorQueryThread(t, query);
    }
  }

  private int getThreadIndex() {
    return removeYoungest ? threads.size() - 1 : 0;
  }
}
