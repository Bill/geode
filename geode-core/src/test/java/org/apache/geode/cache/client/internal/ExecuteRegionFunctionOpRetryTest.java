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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FUNCTION_HAS_RESULT;
import static org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FUNCTION_NAME;
import static org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.OPTIMIZE_FOR_WRITE_SETTING;
import static org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.REGION_NAME;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.HashSet;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.OngoingStubbing;

import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FailureMode;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.FunctionIdentifierType;
import org.apache.geode.cache.client.internal.ExecuteFunctionTestSupport.HAStatus;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Test retry counts on multi-hop on onRegion() function execution (via ExecuteRegionFunctionOp).
 * Ensure they are never infinite!
 */
@Category({ClientServerTest.class})
@RunWith(JUnitParamsRunner.class)
public class ExecuteRegionFunctionOpRetryTest {

  private ExecuteFunctionTestSupport testSupport;

  @Test
  @Parameters({
      "NOT_HA, OBJECT_REFERENCE, -1, 1",
      "NOT_HA, OBJECT_REFERENCE, 0, 1",
      "NOT_HA, OBJECT_REFERENCE, 3, 1",
      "NOT_HA, STRING, -1, 1",
      "NOT_HA, STRING, 0, 1",
      "NOT_HA, STRING, 3, 1",
      "HA, OBJECT_REFERENCE, -1, 1",
      "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 1",
      "HA, STRING, -1, 1",
      "HA, STRING, 0, 1",
      "HA, STRING, 3, 1",
  })
  public void executeDoesNotRetryOnServerOperationException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_SERVER_OPERATION_EXCEPTION);
  }

  @Test
  @Parameters({
      "NOT_HA, OBJECT_REFERENCE, -1, 1",
      "NOT_HA, OBJECT_REFERENCE, 0, 1",
      "NOT_HA, OBJECT_REFERENCE, 3, 1",
      "NOT_HA, STRING, -1, 1",
      "NOT_HA, STRING, 0, 1",
      "NOT_HA, STRING, 3, 1",
      "HA, OBJECT_REFERENCE, -1, 1",
      "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 1",
      "HA, STRING, -1, 1",
      "HA, STRING, 0, 1",
      "HA, STRING, 3, 1",
  })
  public void executeDoesNotRetryOnNoServerAvailableException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_NO_AVAILABLE_SERVERS_EXCEPTION);
  }

  @Test
  @Parameters({
      "NOT_HA, OBJECT_REFERENCE, -1, 1",
      "NOT_HA, OBJECT_REFERENCE, 0, 1",
      "NOT_HA, OBJECT_REFERENCE, 3, 1",
      "NOT_HA, STRING, -1, 1",
      "NOT_HA, STRING, 0, 1",
      "NOT_HA, STRING, 3, 1",
      "HA, OBJECT_REFERENCE, -1, 2",
      "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 4",
      "HA, STRING, -1, 2",
      "HA, STRING, 0, 1",
      "HA, STRING, 3, 4",
  })
  public void executeWithServerConnectivityException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_SERVER_CONNECTIVITY_EXCEPTION);
  }

  @Test
  @Parameters({
      "NOT_HA, OBJECT_REFERENCE, -1, 1",
      "NOT_HA, OBJECT_REFERENCE, 0, 1",
      "NOT_HA, OBJECT_REFERENCE, 3, 1",
      "NOT_HA, STRING, -1, 1",
      "NOT_HA, STRING, 0, 1",
      "NOT_HA, STRING, 3, 1",
      /*
       * For these HA cases the counts may seem odd (one or two larger than you might expect)
       * But that's because execute() treats a try that throws
       * InternalFunctionInvocationTargetException as no try at all. Since our mock throws
       * that exception first (and then throws ServerConnectivityException) the pool mock
       * sees one extra call to its execute method.
       */
      "HA, OBJECT_REFERENCE, -1, 3",
      "HA, OBJECT_REFERENCE, 0, 2",
      "HA, OBJECT_REFERENCE, 3, 5",
      "HA, STRING, -1, 3",
      "HA, STRING, 0, 2",
      "HA, STRING, 3, 5",
  })
  public void executeWithInternalFunctionInvocationTargetExceptionCallsReexecute(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runExecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_INTERNAL_FUNCTION_INVOCATION_TARGET_EXCEPTION);
  }

  @Test
  @Parameters({
      "HA, OBJECT_REFERENCE, -1, 1",
      "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 1",
      "HA, STRING, -1, 1",
      "HA, STRING, 0, 1",
      "HA, STRING, 3, 1",
  })
  public void reexecuteDoesNotRetryOnServerOperationException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runReexecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_SERVER_OPERATION_EXCEPTION);
  }

  @Test
  @Parameters({
      "HA, OBJECT_REFERENCE, -1, 1",
      "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 1",
      "HA, STRING, -1, 1",
      "HA, STRING, 0, 1",
      "HA, STRING, 3, 1",
  })
  public void reexecuteDoesNotRetryOnNoServerAvailableException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runReexecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_NO_AVAILABLE_SERVERS_EXCEPTION);
  }

  @Test
  @Parameters({
      "HA, OBJECT_REFERENCE, -1, 2",
      "HA, OBJECT_REFERENCE, 0, 1",
      "HA, OBJECT_REFERENCE, 3, 4",
      "HA, STRING, -1, 2",
      "HA, STRING, 0, 1",
      "HA, STRING, 3, 4",
  })
  public void reexecuteWithServerConnectivityException(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runReexecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_SERVER_CONNECTIVITY_EXCEPTION);
  }

  @Test
  @Parameters({
      /*
       * For these HA cases the counts may seem odd (one or two larger than you might expect)
       * But that's because execute() treats a try that throws
       * InternalFunctionInvocationTargetException as no try at all. Since our mock throws
       * that exception first (and then throws ServerConnectivityException) the pool mock
       * sees one extra call to its execute method.
       */
      "HA, OBJECT_REFERENCE, -1, 3",
      "HA, OBJECT_REFERENCE, 0, 2",
      "HA, OBJECT_REFERENCE, 3, 5",
      "HA, STRING, -1, 3",
      "HA, STRING, 0, 2",
      "HA, STRING, 3, 5",
  })
  public void reexecuteWithInternalFunctionInvocationTargetExceptionCallsReexecute(
      final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectTries) {

    runReexecuteTest(haStatus, functionIdentifierType, retryAttempts, expectTries,
        FailureMode.THROW_INTERNAL_FUNCTION_INVOCATION_TARGET_EXCEPTION);
  }

  private void runExecuteTest(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts, final int expectTries,
      final FailureMode failureModeArg) {

    testSupport = new ExecuteFunctionTestSupport(haStatus, failureModeArg,
        retryAttempts, (final PoolImpl pool, final FailureMode failureMode) -> {
          /*
           * We know execute() handles three kinds of exception from the pool:
           *
           * InternalFunctionInvocationTargetException
           *
           * keep trying without regard to retry attempt limit
           *
           * ServerOperationException | NoAvailableServersException
           *
           * re-throw
           *
           * ServerConnectivityException
           *
           * keep trying up to retry attempt limit
           */
          final OngoingStubbing<Object> when = when(pool
              .execute(ArgumentMatchers.<AbstractOp>any(), ArgumentMatchers.anyInt()));
          switch (failureMode) {
            case NO_FAILURE:
              when.thenReturn(null);
              break;
            case THROW_SERVER_CONNECTIVITY_EXCEPTION:
              when.thenThrow(new ServerConnectivityException("testing"));
              break;
            case THROW_SERVER_OPERATION_EXCEPTION:
              when.thenThrow(new ServerOperationException("testing"));
              break;
            case THROW_NO_AVAILABLE_SERVERS_EXCEPTION:
              when.thenThrow(new NoAvailableServersException("testing"));
              break;
            case THROW_INTERNAL_FUNCTION_INVOCATION_TARGET_EXCEPTION:
              /*
               * The product assumes that InternalFunctionInvocationTargetException will only be
               * encountered
               * when the target cache is going down. That condition is transient so the product
               * assume
               * it's
               * ok to keep trying forever. In order to test this situation (and for the test to not
               * hang)
               * we throw this exception first, then we throw ServerConnectivityException
               */
              when.thenThrow(new InternalFunctionInvocationTargetException("testing"))
                  .thenThrow(new ServerConnectivityException("testing"));
              break;
            default:
              throw new AssertionError("unknown FailureMode type: " + failureMode);
          }
        });

    executeFunctionMultiHopAndValidate(haStatus, functionIdentifierType, retryAttempts,
        testSupport.getExecutablePool(),
        testSupport.getFunction(),
        testSupport.getExecutor(), testSupport.getResultCollector(), expectTries);
  }

  private void runReexecuteTest(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts, final int expectTries,
      final FailureMode failureModeArg) {

    testSupport = new ExecuteFunctionTestSupport(haStatus, failureModeArg,
        retryAttempts, ExecuteFunctionTestSupport::mockThrowOnExecute2);

    reexecuteFunctionMultiHopAndValidate(haStatus, functionIdentifierType, retryAttempts,
        testSupport.getExecutablePool(),
        testSupport.getFunction(),
        testSupport.getExecutor(), testSupport.getResultCollector(), expectTries);
  }

  private void executeFunctionMultiHopAndValidate(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final PoolImpl executablePool,
      final Function<Integer> function,
      final ServerRegionFunctionExecutor executor,
      final ResultCollector<Integer, Collection<Integer>> resultCollector,
      final int expectTries) {
    switch (functionIdentifierType) {
      case STRING:
        ignoreServerConnectivityException(
            () -> ExecuteRegionFunctionOp.execute(executablePool, REGION_NAME, FUNCTION_NAME,
                executor, resultCollector, FUNCTION_HAS_RESULT, retryAttempts,
                testSupport.toBoolean(haStatus),
                OPTIMIZE_FOR_WRITE_SETTING));
        break;
      case OBJECT_REFERENCE:
        ignoreServerConnectivityException(
            () -> ExecuteRegionFunctionOp.execute(executablePool, REGION_NAME, function,
                executor, resultCollector, FUNCTION_HAS_RESULT, retryAttempts));
        break;
      default:
        throw new AssertionError("unknown FunctionIdentifierType type: " + functionIdentifierType);
    }

    verify(testSupport.getExecutablePool(), times(expectTries)).execute(
        ArgumentMatchers.<AbstractOp>any(),
        ArgumentMatchers.anyInt());
  }

  private void reexecuteFunctionMultiHopAndValidate(final HAStatus haStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final PoolImpl executablePool,
      final Function<Integer> function,
      final ServerRegionFunctionExecutor executor,
      final ResultCollector<Integer, Collection<Integer>> resultCollector,
      final int expectTries) {
    switch (functionIdentifierType) {
      case STRING:
        ignoreServerConnectivityException(
            () -> ExecuteRegionFunctionOp.reexecute(executablePool, REGION_NAME, FUNCTION_NAME,
                executor, resultCollector, FUNCTION_HAS_RESULT, new HashSet<>(),
                retryAttempts, testSupport.toBoolean(haStatus),
                OPTIMIZE_FOR_WRITE_SETTING));
        break;
      case OBJECT_REFERENCE:
        ignoreServerConnectivityException(
            () -> ExecuteRegionFunctionOp.reexecute(executablePool, REGION_NAME, function,
                executor, resultCollector, FUNCTION_HAS_RESULT, new HashSet<>(), retryAttempts));
        break;
      default:
        throw new AssertionError("unknown FunctionIdentifierType type: " + functionIdentifierType);
    }

    verify(testSupport.getExecutablePool(), times(expectTries)).execute(
        ArgumentMatchers.<AbstractOp>any(),
        ArgumentMatchers.anyInt());
  }

  /*
   * We could be pedantic about this exception but that's not the purpose of the retryTest()
   */
  private void ignoreServerConnectivityException(final Runnable runnable) {
    try {
      runnable.run();
    } catch (final ServerConnectivityException e) {
      // ok
    }
  }
}
