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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.ExecuteRegionFunctionSingleHopOp.ExecuteRegionFunctionSingleHopOpImpl;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.execute.FunctionStats;
import org.apache.geode.internal.cache.execute.InternalFunctionException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.MemberMappedArgument;
import org.apache.geode.internal.cache.execute.ServerRegionFunctionExecutor;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.logging.LogService;

/**
 * Does a Execution of function on server region.<br>
 * It alos gets result from the server
 *
 * @since GemFire 5.8Beta
 */
public class ExecuteRegionFunctionOp {

  private static final Logger logger = LogService.getLogger();

  private ExecuteRegionFunctionOp() {
    // no instances allowed
  }

  /**
   * Does a execute Function on a server using connections from the given pool to communicate with
   * the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the put on
   * @param function to be executed
   * @param serverRegionExecutor which will return argument and filter
   * @param resultCollector is used to collect the results from the Server
   */
  public static void execute(ExecutablePool pool, String region, Function function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, int mRetryAttempts) {

    AbstractOp op = new ExecuteRegionFunctionOpImpl(region, function, serverRegionExecutor,
        resultCollector, hasResult, new HashSet<String>());
    boolean reexecute = false;
    boolean reexecuteForServ = false;
    Set<String> failedNodes = new HashSet<String>();
    AbstractOp reexecOp = null;

    int maxRetryAttempts = mRetryAttempts;
    if (!function.isHA()) {
      maxRetryAttempts = 0;
    }

    do {
      try {
        if (reexecuteForServ) {
          reexecOp = new ExecuteRegionFunctionOpImpl((ExecuteRegionFunctionOpImpl) op,
              (byte) 1/* isReExecute */, failedNodes);
          pool.execute(reexecOp, 0);
        } else {
          pool.execute(op, 0);
        }
        reexecute = false;
        reexecuteForServ = false;
      } catch (InternalFunctionInvocationTargetException e) {
        reexecute = true;
        resultCollector.clearResults();
        Set<String> failedNodesIds = e.getFailedNodeSet();
        failedNodes.clear();
        if (failedNodesIds != null) {
          failedNodes.addAll(failedNodesIds);
        }
      } catch (ServerOperationException | NoAvailableServersException failedException) {
        throw failedException;
      } catch (ServerConnectivityException se) {

        if (maxRetryAttempts == PoolFactory.DEFAULT_RETRY_ATTEMPTS) {
          // If the retryAttempt is set to default(-1). Try it on all servers once.
          // Calculating number of servers when function is re-executed as it involves
          // messaging locator.
          maxRetryAttempts = ((PoolImpl) pool).getConnectionSource().getAllServers().size() - 1;
        }

        if ((maxRetryAttempts--) < 1) {
          throw se;
        }

        reexecuteForServ = true;
        resultCollector.clearResults();
        failedNodes.clear();
      }
    } while (reexecuteForServ);

    if (reexecute && function.isHA()) {
      ExecuteRegionFunctionOp.reexecute(pool, region, function, serverRegionExecutor,
          resultCollector, hasResult, failedNodes, maxRetryAttempts);
    }
  }

  public static void execute(ExecutablePool pool, String region, String function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, int mRetryAttempts, boolean isHA, boolean optimizeForWrite) {

    AbstractOp op = new ExecuteRegionFunctionOpImpl(region, function, serverRegionExecutor,
        resultCollector, hasResult, new HashSet<String>(), isHA, optimizeForWrite, true);
    boolean reexecute = false;
    boolean reexecuteForServ = false;
    Set<String> failedNodes = new HashSet<String>();
    AbstractOp reexecOp = null;

    int maxRetryAttempts = mRetryAttempts;
    if (isHA) {
      maxRetryAttempts = 0;
    }

    do {
      try {
        if (reexecuteForServ) {
          reexecOp = new ExecuteRegionFunctionOpImpl((ExecuteRegionFunctionOpImpl) op,
              (byte) 1/* isReExecute */, failedNodes);
          pool.execute(reexecOp, 0);
        } else {
          pool.execute(op, 0);
        }
        reexecute = false;
        reexecuteForServ = false;
      } catch (InternalFunctionInvocationTargetException e) {
        reexecute = true;
        resultCollector.clearResults();
        Set<String> failedNodesIds = e.getFailedNodeSet();
        failedNodes.clear();
        if (failedNodesIds != null) {
          failedNodes.addAll(failedNodesIds);
        }
      } catch (ServerOperationException | NoAvailableServersException failedException) {
        throw failedException;
      } catch (ServerConnectivityException se) {

        if (maxRetryAttempts == PoolFactory.DEFAULT_RETRY_ATTEMPTS) {
          // If the retryAttempt is set to default(-1). Try it on all servers once.
          // Calculating number of servers when function is re-executed as it involves
          // messaging locator.
          maxRetryAttempts = ((PoolImpl) pool).getConnectionSource().getAllServers().size() - 1;
        }

        if ((maxRetryAttempts--) < 1) {
          throw se;
        }

        reexecuteForServ = true;
        resultCollector.clearResults();
        failedNodes.clear();
      }
    } while (reexecuteForServ);

    if (reexecute && isHA) {
      ExecuteRegionFunctionOp.reexecute(pool, region, function, serverRegionExecutor,
          resultCollector, hasResult, failedNodes, maxRetryAttempts, isHA, optimizeForWrite);
    }
  }

  public static void reexecute(ExecutablePool pool, String region, Function function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, Set<String> failedNodes, int retryAttempts) {

    AbstractOp op = new ExecuteRegionFunctionOpImpl(region, function, serverRegionExecutor,
        resultCollector, hasResult, new HashSet<String>());
    boolean reexecute = true;
    int maxRetryAttempts = retryAttempts;

    do {
      AbstractOp reExecuteOp = new ExecuteRegionFunctionOpImpl((ExecuteRegionFunctionOpImpl) op,
          (byte) 1/* isReExecute */, failedNodes);

      try {
        pool.execute(reExecuteOp, 0);
        reexecute = false;
      } catch (InternalFunctionInvocationTargetException e) {
        resultCollector.clearResults();
        Set<String> failedNodesIds = e.getFailedNodeSet();
        failedNodes.clear();
        if (failedNodesIds != null) {
          failedNodes.addAll(failedNodesIds);
        }
      } catch (ServerOperationException | NoAvailableServersException failedException) {
        throw failedException;
      } catch (ServerConnectivityException se) {

        if (maxRetryAttempts == PoolFactory.DEFAULT_RETRY_ATTEMPTS) {
          // If the retryAttempt is set to default(-1). Try it on all servers once.
          // Calculating number of servers when function is re-executed as it involves
          // messaging locator.
          maxRetryAttempts = ((PoolImpl) pool).getConnectionSource().getAllServers().size() - 1;
        }

        if ((maxRetryAttempts--) < 1) {
          throw se;
        }

        resultCollector.clearResults();
        failedNodes.clear();
      }
    } while (reexecute);
  }

  public static void reexecute(ExecutablePool pool, String region, String function,
      ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector resultCollector,
      byte hasResult, Set<String> failedNodes, int retryAttempts, boolean isHA,
      boolean optimizeForWrite) {

    AbstractOp op = new ExecuteRegionFunctionOpImpl(region, function, serverRegionExecutor,
        resultCollector, hasResult, new HashSet<String>(), isHA, optimizeForWrite, true);
    boolean reexecute = true;
    int maxRetryAttempts = retryAttempts;

    do {
      AbstractOp reExecuteOp = new ExecuteRegionFunctionOpImpl((ExecuteRegionFunctionOpImpl) op,
          (byte) 1/* isReExecute */, failedNodes);

      try {
        pool.execute(reExecuteOp, 0);
        reexecute = false;
      } catch (InternalFunctionInvocationTargetException e) {
        resultCollector.clearResults();
        Set<String> failedNodesIds = e.getFailedNodeSet();
        failedNodes.clear();
        if (failedNodesIds != null) {
          failedNodes.addAll(failedNodesIds);
        }
      } catch (ServerOperationException | NoAvailableServersException failedException) {
         throw failedException;
      } catch (ServerConnectivityException se) {

        if (maxRetryAttempts == PoolFactory.DEFAULT_RETRY_ATTEMPTS) {
          // If the retryAttempt is set to default(-1). Try it on all servers once.
          // Calculating number of servers when function is re-executed as it involves
          // messaging locator.
          maxRetryAttempts = ((PoolImpl) pool).getConnectionSource().getAllServers().size() - 1;
        }

        if ((maxRetryAttempts--) < 1) {
          throw se;
        }

        resultCollector.clearResults();
        failedNodes.clear();
      }
    } while (reexecute);
  }

  static class ExecuteRegionFunctionOpImpl extends AbstractOp {

    // To collect the results from the server
    private final ResultCollector resultCollector;

    // To get the instance of the Function Statistics we need the function name or instance
    private Function function;

    private byte isReExecute = 0;

    private final String regionName;

    private final ServerRegionFunctionExecutor executor;

    private final byte hasResult;

    private Set<String> failedNodes = new HashSet<String>();

    private final String functionId;

    private final boolean executeOnBucketSet;

    private final boolean isHA;

    private FunctionException functionException;


    public ExecuteRegionFunctionOpImpl(String region, Function function,
        ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector rc, byte hasResult,
        Set<String> removedNodes) {
      super(MessageType.EXECUTE_REGION_FUNCTION,
          8 + serverRegionExecutor.getFilter().size() + removedNodes.size());
      Set routingObjects = serverRegionExecutor.getFilter();
      Object args = serverRegionExecutor.getArguments();
      byte functionState = AbstractExecution.getFunctionState(function.isHA(), function.hasResult(),
          function.optimizeForWrite());
      MemberMappedArgument memberMappedArg = serverRegionExecutor.getMemberMappedArgument();

      addBytes(functionState);
      getMessage().addStringPart(region, true);
      if (serverRegionExecutor.isFnSerializationReqd()) {
        getMessage().addStringOrObjPart(function);
      } else {
        getMessage().addStringOrObjPart(function.getId());
      }
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      this.executeOnBucketSet = serverRegionExecutor.getExecuteOnBucketSetFlag();
      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);

      getMessage().addBytesPart(new byte[] {flags});
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        getMessage().addStringOrObjPart(key);
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }

      this.resultCollector = rc;
      this.regionName = region;
      this.function = function;
      this.functionId = function.getId();
      this.executor = serverRegionExecutor;
      this.hasResult = functionState;
      this.failedNodes = removedNodes;
      this.isHA = function.isHA();
    }

    // For testing only
    ExecuteRegionFunctionOpImpl() {
      super(MessageType.EXECUTE_REGION_FUNCTION,
          0);
      resultCollector = null;
      function = null;
      isReExecute = (byte) 0;
      regionName = "";
      executor = null;
      hasResult = (byte) 0;
      failedNodes = null;
      functionId = null;
      executeOnBucketSet = true;
      isHA = true;
    }

    public ExecuteRegionFunctionOpImpl(String region, String function,
        ServerRegionFunctionExecutor serverRegionExecutor, ResultCollector rc, byte hasResult,
        Set<String> removedNodes, boolean isHA, boolean optimizeForWrite,
        boolean calculateFnState) {
      super(MessageType.EXECUTE_REGION_FUNCTION,
          8 + serverRegionExecutor.getFilter().size() + removedNodes.size());
      Set routingObjects = serverRegionExecutor.getFilter();
      byte functionState = hasResult;
      if (calculateFnState) {
        functionState = AbstractExecution.getFunctionState(isHA,
            hasResult == (byte) 1 ? true : false, optimizeForWrite);
      }
      Object args = serverRegionExecutor.getArguments();
      MemberMappedArgument memberMappedArg = serverRegionExecutor.getMemberMappedArgument();
      addBytes(functionState);
      getMessage().addStringPart(region, true);
      getMessage().addStringOrObjPart(function);
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);

      this.executeOnBucketSet = serverRegionExecutor.getExecuteOnBucketSetFlag();
      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);

      getMessage().addBytesPart(new byte[] {flags});
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        getMessage().addStringOrObjPart(key);
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }

      this.resultCollector = rc;
      this.regionName = region;
      this.functionId = function;
      this.executor = serverRegionExecutor;
      this.hasResult = functionState;
      this.failedNodes = removedNodes;
      this.isHA = isHA;
    }

    public ExecuteRegionFunctionOpImpl(ExecuteRegionFunctionSingleHopOpImpl newop) {
      this(newop.getRegionName(), newop.getFunctionId(), newop.getExecutor(),
          newop.getResultCollector(), newop.getHasResult(), new HashSet<String>(), newop.isHA(),
          newop.optimizeForWrite(), false);
    }

    public ExecuteRegionFunctionOpImpl(ExecuteRegionFunctionOpImpl op, byte isReExecute,
        Set<String> removedNodes) {
      super(MessageType.EXECUTE_REGION_FUNCTION,
          8 + op.executor.getFilter().size() + removedNodes.size());
      this.isReExecute = isReExecute;
      this.resultCollector = op.resultCollector;
      this.function = op.function;
      this.functionId = op.functionId;
      this.regionName = op.regionName;
      this.executor = op.executor;
      this.hasResult = op.hasResult;
      this.failedNodes = op.failedNodes;
      this.executeOnBucketSet = op.executeOnBucketSet;
      this.isHA = op.isHA;
      if (isReExecute == 1) {
        this.resultCollector.endResults();
        this.resultCollector.clearResults();
      }

      Set routingObjects = executor.getFilter();
      Object args = executor.getArguments();
      MemberMappedArgument memberMappedArg = executor.getMemberMappedArgument();
      getMessage().clear();
      addBytes(this.hasResult);
      getMessage().addStringPart(this.regionName, true);
      if (executor.isFnSerializationReqd()) {
        getMessage().addStringOrObjPart(function);
      } else {
        getMessage().addStringOrObjPart(functionId);
      }
      getMessage().addObjPart(args);
      getMessage().addObjPart(memberMappedArg);
      byte flags = ExecuteFunctionHelper.createFlags(executeOnBucketSet, isReExecute);

      getMessage().addBytesPart(new byte[] {flags});
      getMessage().addIntPart(routingObjects.size());
      for (Object key : routingObjects) {
        getMessage().addStringOrObjPart(key);
      }
      getMessage().addIntPart(removedNodes.size());
      for (Object nodes : removedNodes) {
        getMessage().addStringOrObjPart(nodes);
      }
    }

    private void addBytes(byte functionStateOrHasResult) {
      if (ConnectionImpl
          .getClientFunctionTimeout() == ConnectionImpl.DEFAULT_CLIENT_FUNCTION_TIMEOUT) {
        getMessage().addBytesPart(new byte[] {functionStateOrHasResult});
      } else {
        byte[] bytes = new byte[5];
        bytes[0] = functionStateOrHasResult;
        Part.encodeInt(ConnectionImpl.getClientFunctionTimeout(), bytes, 1);
        getMessage().addBytesPart(bytes);
      }
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      ChunkedMessage executeFunctionResponseMsg = (ChunkedMessage) msg;
      // Read the header which describes the type of message following
      try {
        executeFunctionResponseMsg.readHeader();
        switch (executeFunctionResponseMsg.getMessageType()) {
          case MessageType.EXECUTE_REGION_FUNCTION_RESULT:
            final boolean isDebugEnabled = logger.isDebugEnabled();
            if (isDebugEnabled) {
              logger.debug(
                  "ExecuteRegionFunctionOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_RESULT. The number of parts are : {}",
                  executeFunctionResponseMsg.getNumberOfParts());
            }
            // Read the chunk
            boolean throwServerOp = false;
            do {
              executeFunctionResponseMsg.receiveChunk();
              Object resultResponse = executeFunctionResponseMsg.getPart(0).getObject();
              Object result;
              if (resultResponse instanceof ArrayList) {
                result = ((ArrayList) resultResponse).get(0);
              } else {
                result = resultResponse;
              }

              // if the function is HA throw exceptions
              // if nonHA collect these exceptions and wait till you get last chunk
              if (result instanceof FunctionException) {
                FunctionException ex = ((FunctionException) result);
                if (ex instanceof InternalFunctionException) {
                  Throwable cause = ex.getCause();
                  DistributedMember memberID =
                      (DistributedMember) ((ArrayList) resultResponse).get(1);
                  this.resultCollector.addResult(memberID, cause);
                  FunctionStats
                      .getFunctionStats(this.functionId, this.executor.getRegion().getSystem())
                      .incResultsReceived();
                  continue;
                } else if (((FunctionException) result)
                    .getCause() instanceof InternalFunctionInvocationTargetException) {
                  InternalFunctionInvocationTargetException ifite =
                      (InternalFunctionInvocationTargetException) ex.getCause();
                  this.failedNodes.addAll(ifite.getFailedNodeSet());
                  addFunctionException((FunctionException) result);
                } else {
                  addFunctionException((FunctionException) result);
                }
              } else if (result instanceof Throwable) {
                Throwable t = (Throwable) result;
                if (this.functionException == null) {
                  if (result instanceof BucketMovedException) {
                    FunctionInvocationTargetException fite;
                    if (isHA) {
                      fite = new InternalFunctionInvocationTargetException(
                          ((BucketMovedException) result).getMessage());
                    } else {
                      fite = new FunctionInvocationTargetException(
                          ((BucketMovedException) result).getMessage());
                    }
                    this.functionException = new FunctionException(fite);
                    this.functionException.addException(fite);
                  } else if (result instanceof CacheClosedException) {
                    FunctionInvocationTargetException fite;
                    if (isHA) {
                      fite = new InternalFunctionInvocationTargetException(
                          ((CacheClosedException) result).getMessage());
                    } else {
                      fite = new FunctionInvocationTargetException(
                          ((CacheClosedException) result).getMessage());
                    }
                    if (resultResponse instanceof ArrayList) {
                      DistributedMember memberID =
                          (DistributedMember) ((ArrayList) resultResponse).get(1);
                      this.failedNodes.add(memberID.getId());
                    }
                    this.functionException = new FunctionException(fite);
                    this.functionException.addException(fite);
                  } else {
                    throwServerOp = true;
                    this.functionException = new FunctionException(t);
                    this.functionException.addException(t);
                  }
                } else {
                  this.functionException.addException(t);
                }
              } else {
                DistributedMember memberID =
                    (DistributedMember) ((ArrayList) resultResponse).get(1);
                this.resultCollector.addResult(memberID, result);
                FunctionStats
                    .getFunctionStats(this.functionId, this.executor.getRegion().getSystem())
                    .incResultsReceived();
              }
            } while (!executeFunctionResponseMsg.isLastChunk());
            if (isDebugEnabled) {
              logger.debug(
                  "ExecuteRegionFunctionOpImpl#processResponse: received all the results from server successfully.");
            }


            if (isHA && throwServerOp) {
              String s = "While performing a remote " + getOpName();
              throw new ServerOperationException(s, this.functionException);
            }

            // add all the exceptions here.
            if (this.functionException != null) {
              throw this.functionException;
            }
            this.resultCollector.endResults();
            return null;

          case MessageType.EXCEPTION:
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteRegionFunctionOpImpl#processResponse: received message of type EXCEPTION. The number of parts are : {}",
                  executeFunctionResponseMsg.getNumberOfParts());
            }

            // Read the chunk
            executeFunctionResponseMsg.receiveChunk();
            Part part0 = executeFunctionResponseMsg.getPart(0);
            Object obj = part0.getObject();
            if (obj instanceof FunctionException) {
              FunctionException ex = ((FunctionException) obj);
              if (((FunctionException) obj)
                  .getCause() instanceof InternalFunctionInvocationTargetException) {
                InternalFunctionInvocationTargetException ifite =
                    (InternalFunctionInvocationTargetException) ex.getCause();
                this.failedNodes.addAll(ifite.getFailedNodeSet());
              }
              throw ex;
            } else if (obj instanceof Throwable) {
              String s = "While performing a remote " + getOpName();
              throw new ServerOperationException(s, (Throwable) obj);
            }
            break;
          case MessageType.EXECUTE_REGION_FUNCTION_ERROR:
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "ExecuteRegionFunctionOpImpl#processResponse: received message of type EXECUTE_REGION_FUNCTION_ERROR");
            }
            // Read the chunk
            executeFunctionResponseMsg.receiveChunk();
            String errorMessage = executeFunctionResponseMsg.getPart(0).getString();
            throw new ServerOperationException(errorMessage);
          default:
            throw new InternalGemFireError(
                "Unknown message type " + executeFunctionResponseMsg.getMessageType());
        }
      } finally {
        executeFunctionResponseMsg.clear();
      }
      return null;
    }

    void addFunctionException(final FunctionException result) {
      if (result.getCause() instanceof FunctionInvocationTargetException) {
        if (this.functionException == null) {
          this.functionException = result;
        }
        this.functionException.addException(result.getCause());
      } else if (result instanceof FunctionInvocationTargetException) {
        if (this.functionException == null) {
          this.functionException = new FunctionException(result);
        }
        this.functionException.addException(result);
      } else {
        if (this.functionException == null) {
          this.functionException = result;
        }
        this.functionException.addException(result);
      }
    }

    FunctionException getFunctionException() {
      return functionException;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.EXECUTE_REGION_FUNCTION_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startExecuteFunction();
    }

    protected String getOpName() {
      return "executeRegionFunction";
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endExecuteFunctionSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endExecuteFunction(start, hasTimedOut(), hasFailed());
    }

    @Override
    protected Message createResponseMessage() {
      return new ChunkedMessage(1, Version.CURRENT);
    }

  }
}
