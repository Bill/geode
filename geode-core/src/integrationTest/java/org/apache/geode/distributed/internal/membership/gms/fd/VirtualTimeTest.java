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
package org.apache.geode.distributed.internal.membership.gms.fd;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

public class VirtualTimeTest {

  private VirtualTime virtualTime;
  private TestScheduler scheduler;

  @Before
  public void before() {
    virtualTime = new VirtualTime();
    scheduler = new TestScheduler(virtualTime);
  }

  @Test
  public void schedulerHoldsReadyTaskBeforeTriggerActions() {

    final AtomicBoolean flag = new AtomicBoolean(false);

    scheduler.schedule(()->flag.set(true), 0, MILLISECONDS);

    assertThat(flag.get()).isFalse().describedAs("scheduler ran task prematurely");
  }

  @Test
  public void triggerActionsRunsReadyTask() {

    final AtomicBoolean flag = new AtomicBoolean(false);

    scheduler.schedule(()->flag.set(true), 0, MILLISECONDS);

    scheduler.triggerActions();

    assertThat(flag.get()).isTrue().describedAs("scheduler failed to run task");
  }

  @Test
  public void triggerActionsHoldsFutureTask() {

    final AtomicBoolean flag = new AtomicBoolean(false);

    scheduler.schedule(()->flag.set(true), 1, MILLISECONDS);

    scheduler.triggerActions();

    assertThat(flag.get()).isFalse();
  }

  @Test
  public void recursiveTaskScheduling() {

    final AtomicBoolean flag = new AtomicBoolean(false);

    scheduler.schedule(()->{
      scheduler.schedule(()->flag.set(true), 0, MILLISECONDS);
    }, 0, MILLISECONDS);

    scheduler.triggerActions();

    assertThat(flag.get()).isTrue();
  }

  @Test
  public void recursiveTaskSchedulingTwoSteps() {

    final AtomicBoolean flag = new AtomicBoolean(false);

    scheduler.schedule(()->{
      scheduler.schedule(()->flag.set(true), 1, MILLISECONDS);
    }, 0, MILLISECONDS);

    scheduler.triggerActions();

    assertThat(flag.get()).isFalse().describedAs("triggerActions ran task prematurely");

    virtualTime.advance(1, MILLISECONDS);

    scheduler.triggerActions();

    assertThat(flag.get()).isTrue().describedAs("triggerActions failed to run ready task");
  }

  @Test
  public void tasksRunInTimeOrder() {

    final AtomicInteger flag = new AtomicInteger(0);

    // scheduling later task first just to see if that causes a problem
    scheduler.schedule(()->flag.compareAndSet(1,2), 2, MILLISECONDS);
    scheduler.schedule(()->flag.compareAndSet(0,1), 1, MILLISECONDS);

    virtualTime.advance(2, MILLISECONDS);

    scheduler.triggerActions();

    assertThat(flag.get()).isEqualTo(2).describedAs("tasks didn't run in time order");
  }

  @Test
  public void schedulingOrderBreaksTimeOrderTies() {

    final AtomicInteger flag = new AtomicInteger(0);

    scheduler.schedule(()->flag.compareAndSet(0,1), 0, MILLISECONDS);
    scheduler.schedule(()->flag.compareAndSet(1,2), 0, MILLISECONDS);

    scheduler.triggerActions();

    assertThat(flag.get()).isEqualTo(2)
        .describedAs("tasks with same start time didn't run in scheduling order");
  }

}
