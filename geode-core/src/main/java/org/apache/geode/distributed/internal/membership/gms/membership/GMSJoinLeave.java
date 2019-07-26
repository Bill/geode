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
package org.apache.geode.distributed.internal.membership.gms.membership;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.internal.membership.gms.ServiceConfig.MEMBER_REQUEST_COLLECTION_INTERVAL;
import static org.apache.geode.internal.DataSerializableFixedID.LEAVE_REQUEST_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.REMOVE_MEMBER_REQUEST;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimerTask;

import kotlinx.coroutines.CoroutineDispatcher;
import kotlinx.coroutines.CoroutineScope;
import kotlinx.coroutines.CoroutineScopeKt;
import kotlinx.coroutines.Dispatchers;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.GMSUtil;
import org.apache.geode.distributed.internal.membership.gms.ServiceConfig;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorRequest;
import org.apache.geode.distributed.internal.membership.gms.locator.FindCoordinatorResponse;
import org.apache.geode.distributed.internal.membership.gms.messages.HasMemberID;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.NetworkPartitionMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.ViewAckMessage;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.Version;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;

/**
 * GMSJoinLeave handles membership communication with other processes in the distributed system. It
 * replaces the JGroups channel membership services that Geode formerly used for this purpose.
 */
public class GMSJoinLeave implements JoinLeave {

  public static final String BYPASS_DISCOVERY_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "bypass-discovery";

  /**
   * amount of time to wait for responses to FindCoordinatorRequests
   */
  private static final int DISCOVERY_TIMEOUT =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "discovery-timeout", 3000);

  /**
   * amount of time to sleep before trying to join after a failed attempt
   */
  private static final int JOIN_RETRY_SLEEP =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "join-retry-sleep", 1000);

  /**
   * if the locators don't know who the coordinator is we send find-coord requests to this many
   * nodes
   */
  private static final int MAX_DISCOVERY_NODES =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "max-discovery-nodes", 30);

  /**
   * interval for broadcasting the current view to members in case they didn't get it the first time
   */
  private static final long VIEW_BROADCAST_INTERVAL =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "view-broadcast-interval", 60000);

  /**
   * membership logger
   */
  private static final Logger logger = Services.getLogger();
  private static final boolean ALLOW_OLD_VERSION_FOR_TESTING = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "allow_old_members_to_join_for_testing");

  /**
   * the view ID where I entered into membership
   */
  private int birthViewId;

  /**
   * my address
   */
  private InternalDistributedMember localAddress;

  private Services services;

  /**
   * have I connected to the distributed system?
   */
  private volatile boolean isJoined;

  /**
   * guarded by viewInstallationLock
   */
  private volatile boolean isCoordinator;

  /**
   * a synch object that guards view installation
   */
  private final Object viewInstallationLock = new Object();

  /**
   * the currently installed view. Guarded by viewInstallationLock
   */
  private volatile NetView currentView;

  /**
   * the previous view
   **/
  private volatile NetView previousView;

  /**
   * members who we have been declared dead in the current view
   */
  private final Set<InternalDistributedMember> removedMembers = new HashSet<>();

  /**
   * members who we've received a leave message from
   **/
  private final Set<InternalDistributedMember> leftMembers = new HashSet<>();

  /**
   * a new view being installed
   */
  private volatile NetView preparedView;

  private List<HostAddress> locators;

  /**
   * a list of join/leave/crashes
   */
  private final List<DistributionMessage> viewRequests = new LinkedList<DistributionMessage>();

  /**
   * façade over new coroutine-based view creator logic. Unlike old viewCreator,
   * viewCreator2's lifecycle corresponds to that of the GMSJoinLeave object.
   */
  private final ViewCreator2 viewCreator2 = new ViewCreator2();

  /**
   * the established request collection jitter. This can be overridden for testing with
   * delayViewCreationForTest
   */
  long requestCollectionInterval = MEMBER_REQUEST_COLLECTION_INTERVAL;

  /**
   * collects the response to a join request
   */
  private final JoinResponseMessage[] joinResponse = new JoinResponseMessage[1];

  /**
   * collects responses to new views
   */
  ViewReplyProcessor viewProcessor = new ViewReplyProcessor(false);

  /**
   * collects responses to view preparation messages
   */
  ViewReplyProcessor prepareProcessor = new ViewReplyProcessor(true);

  /**
   * whether quorum checks can cause a forced-disconnect
   */
  private boolean quorumRequired = false;

  /**
   * timeout in receiving view acknowledgement
   */
  private int viewAckTimeout;

  /**
   * background thread that creates new membership views
   */
  private ViewCreator viewCreator;

  /**
   * am I shutting down?
   */
  private volatile boolean isStopping;

  /**
   * state of collected artifacts during discovery
   */
  final SearchState searchState = new SearchState();

  /**
   * a collection used to detect unit testing
   */
  Set<String> unitTesting = new HashSet<>();

  /**
   * a test hook to make this member unresponsive
   */
  private volatile boolean playingDead;

  /**
   * the view where quorum was most recently lost
   */
  NetView quorumLostView;

  static class SearchState {
    public int joinedMembersContacted;
    Set<InternalDistributedMember> alreadyTried = new HashSet<>();
    Set<InternalDistributedMember> registrants = new HashSet<>();
    InternalDistributedMember possibleCoordinator;
    int viewId = -100;
    int locatorsContacted = 0;
    boolean hasContactedAJoinedLocator;
    NetView view;
    int lastFindCoordinatorInViewId = -1000;
    final Set<FindCoordinatorResponse> responses = new HashSet<>();
    public int responsesExpected;

    void cleanup() {
      alreadyTried.clear();
      possibleCoordinator = null;
      view = null;
      synchronized (responses) {
        responses.clear();
      }
    }

    public String toString() {
      StringBuffer sb = new StringBuffer(200);
      sb.append("locatorsContacted=").append(locatorsContacted)
          .append("; findInViewResponses=").append(joinedMembersContacted)
          .append("; alreadyTried=").append(alreadyTried).append("; registrants=")
          .append(registrants).append("; possibleCoordinator=").append(possibleCoordinator)
          .append("; viewId=").append(viewId).append("; hasContactedAJoinedLocator=")
          .append(hasContactedAJoinedLocator).append("; view=").append(view).append("; responses=")
          .append(responses);
      return sb.toString();
    }
  }

  Object getViewInstallationLock() {
    return viewInstallationLock;
  }

  /**
   * attempt to join the distributed system loop send a join request to a locator & get a response
   * <p>
   * If the response indicates there's no coordinator it will contain a set of members that have
   * recently contacted it. The "oldest" member is selected as the coordinator based on ID sort
   * order.
   *
   * @return true if successful, false if not
   */
  @Override
  public boolean join() {

    try {
      if (Boolean.getBoolean(BYPASS_DISCOVERY_PROPERTY)) {
        synchronized (viewInstallationLock) {
          becomeCoordinator();
        }
        return true;
      }

      SearchState state = searchState;

      long locatorWaitTime = ((long) services.getConfig().getLocatorWaitTime()) * 1000L;
      long timeout = services.getConfig().getJoinTimeout();
      logger.debug("join timeout is set to {}", timeout);
      long retrySleep = JOIN_RETRY_SLEEP;
      long startTime = System.currentTimeMillis();
      long locatorGiveUpTime = startTime + locatorWaitTime;
      long giveupTime = startTime + timeout;
      int minimumRetriesBeforeBecomingCoordinator = locators.size() * 2;

      for (int tries = 0; !this.isJoined && !this.isStopping; tries++) {
        logger.debug("searching for the membership coordinator");
        boolean found = findCoordinator();
        logger.info("Discovery state after looking for membership coordinator is {}",
            state);
        if (found) {
          logger.info("found possible coordinator {}", state.possibleCoordinator);
          if (localAddress.getNetMember().preferredForCoordinator()
              && state.possibleCoordinator.equals(this.localAddress)) {
            // if we haven't contacted a member of a cluster maybe this node should
            // become the coordinator.
            if (state.joinedMembersContacted <= 0 &&
                (tries >= minimumRetriesBeforeBecomingCoordinator ||
                    state.locatorsContacted >= locators.size())) {
              synchronized (viewInstallationLock) {
                becomeCoordinator();
              }
              return true;
            }
          } else {
            if (attemptToJoin()) {
              return true;
            }
            if (this.isStopping) {
              break;
            }
            if (!state.possibleCoordinator.equals(localAddress)) {
              state.alreadyTried.add(state.possibleCoordinator);
            }
            if (System.currentTimeMillis() > giveupTime) {
              break;
            }
          }
        } else {
          long now = System.currentTimeMillis();
          if (state.locatorsContacted <= 0) {
            if (now > locatorGiveUpTime) {
              // break out of the loop and return false
              break;
            }
            tries = 0;
            giveupTime = now + timeout;
          } else if (now > giveupTime) {
            break;
          }
        }
        try {
          if (found && !state.hasContactedAJoinedLocator) {
            // if locators are restarting they may be handing out IDs from a stale view that
            // we should go through quickly. Otherwise we should sleep a bit to let failure
            // detection select a new coordinator
            if (state.possibleCoordinator.getVmViewId() < 0) {
              logger.debug("sleeping for {} before making another attempt to find the coordinator",
                  retrySleep);
              Thread.sleep(retrySleep);
            } else {
              // since we were given a coordinator that couldn't be used we should keep trying
              tries = 0;
              giveupTime = System.currentTimeMillis() + timeout;
            }
          }
        } catch (InterruptedException e) {
          logger.debug("retry sleep interrupted - giving up on joining the distributed system");
          return false;
        }
      } // for

      if (!this.isJoined) {
        logger.debug("giving up attempting to join the distributed system after "
            + (System.currentTimeMillis() - startTime) + "ms");
      }

      // to preserve old behavior we need to throw a SystemConnectException if
      // unable to contact any of the locators
      if (!this.isJoined && state.hasContactedAJoinedLocator) {
        throw new SystemConnectException("Unable to join the distributed system in "
            + (System.currentTimeMillis() - startTime) + "ms");
      }

      return this.isJoined;
    } finally {
      // notify anyone waiting on the address to be completed
      if (this.isJoined) {
        synchronized (this.localAddress) {
          this.localAddress.notifyAll();
        }
      }
      searchState.cleanup();
    }
  }

  /**
   * send a join request and wait for a reply. Process the reply. This may throw a
   * SystemConnectException or an AuthenticationFailedException
   *
   * @return true if the attempt succeeded, false if it timed out
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WA_NOT_IN_LOOP")
  boolean attemptToJoin() {
    SearchState state = searchState;

    // send a join request to the coordinator and wait for a response
    InternalDistributedMember coord = state.possibleCoordinator;
    if (state.alreadyTried.contains(coord)) {
      logger.info("Probable coordinator is still {} - waiting for a join-response", coord);
    } else {
      logger.info("Attempting to join the distributed system through coordinator " + coord
          + " using address " + this.localAddress);
      int port = services.getHealthMonitor().getFailureDetectionPort();
      JoinRequestMessage req = new JoinRequestMessage(coord, this.localAddress,
          services.getAuthenticator().getCredentials(coord), port,
          services.getMessenger().getRequestId());
      services.getMessenger().send(req);
    }

    JoinResponseMessage response;
    try {
      response = waitForJoinResponse();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }

    if (response == null) {
      if (!isJoined) {
        logger.debug("received no join response");
      }
      return isJoined;
    }

    logger.info("received join response {}", response);
    joinResponse[0] = null;
    String failReason = response.getRejectionMessage();
    if (failReason != null) {
      if (failReason.contains("Rejecting the attempt of a member using an older version")
          || failReason.contains("15806")
          || failReason.contains("ForcedDisconnectException")) {
        throw new SystemConnectException(failReason);
      } else if (failReason.contains("Failed to find credentials")) {
        throw new AuthenticationRequiredException(failReason);
      }
      throw new GemFireSecurityException(failReason);
    }

    throw new RuntimeException("Join Request Failed with response " + response);
  }

  private JoinResponseMessage waitForJoinResponse() throws InterruptedException {
    JoinResponseMessage response;
    synchronized (joinResponse) {
      if (joinResponse[0] == null && !isJoined) {
        // Note that if we give up waiting but a response is on
        // the way we will get the new view and join that way.
        // See installView()
        long timeout = Math.max(services.getConfig().getMemberTimeout(),
            services.getConfig().getJoinTimeout() / 5);
        joinResponse.wait(timeout);
      }
      response = joinResponse[0];

      if (services.getConfig().getDistributionConfig().getSecurityUDPDHAlgo().length() > 0) {
        if (response != null && response.getCurrentView() != null && !isJoined) {
          // reset joinResponse[0]
          joinResponse[0] = null;
          // we got view here that means either we have to wait for
          NetView v = response.getCurrentView();
          InternalDistributedMember coord = v.getCoordinator();
          if (searchState.alreadyTried.contains(coord)) {
            searchState.view = response.getCurrentView();
            // we already sent join request to it..so lets wait some more time here
            // assuming we got this response immediately, so wait for same timeout here..
            long timeout = Math.max(services.getConfig().getMemberTimeout(),
                services.getConfig().getJoinTimeout() / 5);
            joinResponse.wait(timeout);
            response = joinResponse[0];
          } else {
            // try on this coordinator
            searchState.view = response.getCurrentView();
            response = null;
          }
          searchState.view = v;
        }
        if (isJoined) {
          return null;
        }
      }
    }
    return response;
  }

  @Override
  public boolean isMemberLeaving(DistributedMember mbr) {
    if (getPendingRequestIDs(LEAVE_REQUEST_MESSAGE).contains(mbr)
        || getPendingRequestIDs(REMOVE_MEMBER_REQUEST).contains(mbr)
        || !currentView.contains(mbr)) {
      return true;
    }
    synchronized (removedMembers) {
      if (removedMembers.contains(mbr)) {
        return true;
      }
    }
    synchronized (leftMembers) {
      if (leftMembers.contains(mbr)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isQuorumRequired() {
    return quorumRequired;
  }

  /**
   * process a join request from another member. If this is the coordinator this method will enqueue
   * the request for processing in another thread. If this is not the coordinator but the
   * coordinator is known, the message is forwarded to the coordinator.
   *
   * @param joinRequest the request to be processed
   */
  void processMessage(final JoinRequestMessage joinRequest) {
    if (isStopping) {
      return;
    }

    logger.info("Received a join request from {}", joinRequest.getMemberID());

    if (!ALLOW_OLD_VERSION_FOR_TESTING
        && joinRequest.getMemberID().getVersionObject().compareTo(Version.CURRENT) < 0) {
      logger.warn("detected an attempt to start a peer using an older version of the product {}",
          joinRequest.getMemberID());
      JoinResponseMessage m =
          new JoinResponseMessage("Rejecting the attempt of a member using an older version of the "
              + "product to join the distributed system", joinRequest.getRequestId());
      m.setRecipient(joinRequest.getMemberID());
      services.getMessenger().send(m);
      return;
    }

    Object creds = joinRequest.getCredentials();
    String rejection;
    try {
      rejection = services.getAuthenticator().authenticate(joinRequest.getMemberID(),
          (Properties) creds);
    } catch (Exception e) {
      rejection = e.getMessage();
    }
    if (rejection != null && rejection.length() > 0) {
      JoinResponseMessage m = new JoinResponseMessage(rejection, 0);
      m.setRecipient(joinRequest.getMemberID());
      services.getMessenger().send(m);
      return;
    }

    distributeClusterSecretKey(joinRequest);

    recordViewRequest(joinRequest);
    viewCreator2.submit(joinRequest);
  }

  /**
   * Process a Leave request from another member. This may cause this member to become the new
   * membership coordinator. If this is the coordinator a new view will be triggered.
   *
   * @param leaveRequest the request to be processed
   */
  void processMessage(final LeaveRequestMessage leaveRequest) {
    if (isStopping) {
      return;
    }

    logger.info("received leave request from {} for {}", leaveRequest.getSender(),
        leaveRequest.getMemberID());

    NetView v = currentView;
    if (v == null) {
      recordViewRequest(leaveRequest);
      viewCreator2.submit(leaveRequest);
      return;
    }

    InternalDistributedMember mbr = leaveRequest.getMemberID();

    logger.info(() -> "JoinLeave.processMessage(LeaveRequestMessage) invoked.  isCoordinator="
        + isCoordinator
        + "; isStopping=" + isStopping + "; cancelInProgress="
        + services.getCancelCriterion().isCancelInProgress());

    if (!v.contains(mbr) && mbr.getVmViewId() < v.getViewId()) {
      logger.info("ignoring leave request from old member");
      return;
    }

    if (leaveRequest.getMemberID().equals(this.localAddress)) {
      logger.info("I am being told to leave the distributed system by {}",
          leaveRequest.getSender());
      forceDisconnect(leaveRequest.getReason());
      return;
    }

    if (!isCoordinator && !isStopping && !services.getCancelCriterion().isCancelInProgress()) {
      logger.info("Checking to see if I should become coordinator");
      NetView check = new NetView(v, v.getViewId() + 1);
      check.remove(mbr);
      synchronized (removedMembers) {
        check.removeAll(removedMembers);
        check.addCrashedMembers(removedMembers);
      }
      synchronized (leftMembers) {
        leftMembers.add(mbr);
        check.removeAll(leftMembers);
      }
      Collection<InternalDistributedMember> suspectMembers =
          services.getHealthMonitor().getMembersFailingAvailabilityCheck();
      check.removeAll(suspectMembers);
      logger.info("View with removed and left members removed is {}", check);
      if (check.getCoordinator().equals(localAddress)) {
        for (InternalDistributedMember suspect : suspectMembers) {
          final RemoveMemberMessage
              removeRequest =
              new RemoveMemberMessage(localAddress, suspect, "Failed availability check");
          recordViewRequest(
              removeRequest);
          viewCreator2.submit(removeRequest);
        }
        synchronized (viewInstallationLock) {
          becomeCoordinator(mbr);
        }
      }
    } else {
      if (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
        recordViewRequest(leaveRequest);
        viewCreator2.submit(leaveRequest);
        this.viewProcessor.processLeaveRequest(leaveRequest.getMemberID());
        this.prepareProcessor.processLeaveRequest(leaveRequest.getMemberID());
      }
    }
  }

  /**
   * Process a Remove request from another member. This may cause this member to become the new
   * membership coordinator. If this is the coordinator a new view will be triggered.
   *
   * @param removeMemberRequest the request to process
   */
  void processMessage(final RemoveMemberMessage removeMemberRequest) {
    if (isStopping) {
      return;
    }

    NetView v = currentView;
    boolean fromMe =
        removeMemberRequest.getSender() == null || removeMemberRequest.getSender().equals(localAddress);

    InternalDistributedMember mbr = removeMemberRequest.getMemberID();

    if (v != null && !v.contains(removeMemberRequest.getSender())) {
      logger.info("Membership ignoring removal request for " + mbr + " from non-member "
          + removeMemberRequest.getSender());
      return;
    }

    if (v == null) {
      // not yet a member
      return;
    }

    if (!fromMe) {
      logger.info("Membership received a request to remove " + mbr + " from "
          + removeMemberRequest.getSender() + " reason=" + removeMemberRequest.getReason());
    }

    if (mbr.equals(this.localAddress)) {
      // oops - I've been kicked out
      forceDisconnect(removeMemberRequest.getReason());
      return;
    }

    if (getPendingRequestIDs(REMOVE_MEMBER_REQUEST).contains(mbr)) {
      logger.debug("ignoring removal request as I already have a removal request for this member");
      return;
    }

    if (!isCoordinator && !isStopping && !services.getCancelCriterion().isCancelInProgress()) {
      logger.debug("Checking to see if I should become coordinator");
      NetView check = new NetView(v, v.getViewId() + 1);
      synchronized (removedMembers) {
        removedMembers.add(mbr);
        check.addCrashedMembers(removedMembers);
        check.removeAll(removedMembers);
      }
      synchronized (leftMembers) {
        check.removeAll(leftMembers);
      }
      if (check.getCoordinator().equals(localAddress)) {
        synchronized (viewInstallationLock) {
          becomeCoordinator(mbr);
        }
      }
    } else {
      if (!isStopping && !services.getCancelCriterion().isCancelInProgress()) {
        // suspect processing tends to get carried away sometimes during
        // shutdown (especially shutdownAll), so we check for a scheduled shutdown
        // message
        if (!getPendingRequestIDs(LEAVE_REQUEST_MESSAGE).contains(mbr)) {
          recordViewRequest(removeMemberRequest);
          viewCreator2.submit(removeMemberRequest);
          this.viewProcessor.processRemoveRequest(mbr);
          this.prepareProcessor.processRemoveRequest(mbr);
        }
      }
      if (isCoordinator) {
        if (!v.contains(mbr)) {
          // removing a rogue process
          RemoveMemberMessage removeMemberMessage = new RemoveMemberMessage(mbr, mbr,
              removeMemberRequest.getReason());
          services.getMessenger().send(removeMemberMessage);
        }
      }
    }
  }

  private void distributeClusterSecretKey(final JoinRequestMessage joinRequest) {
    if (isCoordinator
        && !services.getConfig().getDistributionConfig().getSecurityUDPDHAlgo().isEmpty()) {
      services.getMessenger().initClusterKey();
      // this will inform about cluster-secret key, as we have authenticated at this point
      final JoinResponseMessage response = new JoinResponseMessage(joinRequest.getSender(),
          services.getMessenger().getClusterSecretKey(), joinRequest.getRequestId());
      services.getMessenger().send(response);
    }
  }

  private void recordViewRequest(DistributionMessage request) {
    try {
      synchronized (viewRequests) {
        logger.debug("Recording the request to be processed in the next membership view");
        viewRequests.add(request);
        viewRequests.notifyAll();
      }
    } catch (RuntimeException | Error t) {
      logger.warn("unable to record a membership view request due to this exception", t);
      throw t;
    }
  }

  // TODO: Bruce thinks I can delete this
  private void sendDHKeys() {
    synchronized (viewRequests) {
      for (final DistributionMessage request : viewRequests) {
        if (request instanceof JoinRequestMessage) {
          distributeClusterSecretKey((JoinRequestMessage) request);
        }
      }
    }
  }

  // for testing purposes, returns a copy of the view requests for verification
  List<DistributionMessage> getViewRequests() {
    System.out.println("in getViewRequests() snapshot from coroutine is: " +
        viewCreator2.snapshot());
    synchronized (viewRequests) {
      return new LinkedList<DistributionMessage>(viewRequests);
    }
  }

  // for testing purposes, returns the view-creation thread
  ViewCreator getViewCreator() {
    return viewCreator;
  }

  /**
   * Yippeee - I get to be the coordinator
   */
  void becomeCoordinator() { // package access for unit testing
    becomeCoordinator(null);
  }

  /**
   * Test hook for delaying the creation of new views. This should be invoked before this member
   * becomes coordinator and creates its ViewCreator thread.
   *
   */
  public void delayViewCreationForTest(int millis) {
    requestCollectionInterval = millis;
  }

  /**
   * Transitions this member into the coordinator role. This must be invoked under a synch on
   * viewInstallationLock that was held at the time the decision was made to become coordinator so
   * that the decision is atomic with actually becoming coordinator.
   *
   * @param oldCoordinator may be null
   */
  private void becomeCoordinator(InternalDistributedMember oldCoordinator) {

    assert Thread.holdsLock(viewInstallationLock);

    if (isCoordinator) {
      return;
    }

    logger.info("This member is becoming the membership coordinator with address {}", localAddress);
    isCoordinator = true;
    org.apache.geode.distributed.internal.membership.gms.interfaces.Locator locator =
        services.getLocator();
    if (locator != null) {
      locator.setIsCoordinator(true);
    }
    sendDHKeys();
    final NetView newView;
    if (currentView == null) {
      // create the initial membership view
      newView = new NetView(this.localAddress);
      newView.setFailureDetectionPort(localAddress,
          services.getHealthMonitor().getFailureDetectionPort());
      this.localAddress.setVmViewId(0);
      installView(newView);
      isJoined = true;
    } else {
      // create and send out a new view
      newView = copyCurrentViewAndAddMyAddress(oldCoordinator);
    }
    createAndStartViewCreator(newView);
    startViewBroadcaster();
  }

  private void createAndStartViewCreator(NetView newView) {
    if (viewCreator == null || viewCreator.isShutdown()) {
      services.getMessenger().initClusterKey();
      viewCreator = new ViewCreator(
          "Geode Membership View Creator",
          this,
          logger,
          viewRequests,
          localAddress,
          viewAckTimeout,
          requestCollectionInterval,
          services.getConfig().getMemberTimeout(),
          services.getLocator(),
          services.getMessenger()
      );
      if (newView != null) {
        viewCreator.setInitialView(newView, newView.getNewMembers(), newView.getShutdownMembers(),
            newView.getCrashedMembers());
      }
      logger.info("ViewCreator starting on:" + localAddress);
      viewCreator.start();
    }
  }

  private NetView copyCurrentViewAndAddMyAddress(InternalDistributedMember oldCoordinator) {
    boolean testing = unitTesting.contains("noRandomViewChange");
    NetView newView;
    Set<InternalDistributedMember> leaving = new HashSet<>();
    Set<InternalDistributedMember> removals;
    synchronized (viewInstallationLock) {
      int rand = testing ? 0 : NetView.RANDOM.nextInt(10);
      int viewNumber = currentView.getViewId() + 5 + rand;
      if (this.localAddress.getVmViewId() < 0) {
        this.localAddress.setVmViewId(viewNumber);
      }
      List<InternalDistributedMember> mbrs = new ArrayList<>(currentView.getMembers());
      if (!mbrs.contains(localAddress)) {
        mbrs.add(localAddress);
      }
      synchronized (this.removedMembers) {
        removals = new HashSet<>(this.removedMembers);
      }
      synchronized (this.leftMembers) {
        leaving.addAll(leftMembers);
      }
      if (oldCoordinator != null && !removals.contains(oldCoordinator)) {
        leaving.add(oldCoordinator);
      }
      mbrs.removeAll(removals);
      mbrs.removeAll(leaving);
      newView = new NetView(this.localAddress, viewNumber, mbrs, leaving, removals);
      newView.setFailureDetectionPorts(currentView);
      newView.setPublicKeys(currentView);
      newView.setFailureDetectionPort(this.localAddress,
          services.getHealthMonitor().getFailureDetectionPort());
    }
    return newView;
  }

  void sendRemoveMessages(List<InternalDistributedMember> removals, List<String> reasons,
      Set<InternalDistributedMember> oldIds) {
    Iterator<String> reason = reasons.iterator();
    for (InternalDistributedMember mbr : removals) {
      // if olds not contains mbr then send remove request
      if (!oldIds.contains(mbr)) {
        RemoveMemberMessage response = new RemoveMemberMessage(mbr, mbr, reason.next());
        services.getMessenger().send(response);
      } else {
        reason.next();
      }
    }
  }

  boolean isShuttingDown() {
    return services.getCancelCriterion().isCancelInProgress()
        || services.getManager().shutdownInProgress() || services.getManager().isShutdownStarted();
  }

  boolean prepareView(NetView view, List<InternalDistributedMember> newMembers)
      throws InterruptedException {
    // GEODE-2193 - don't send a view with new members if we're shutting down
    if (isShuttingDown()) {
      throw new InterruptedException("shutting down");
    }
    return sendView(view, true, this.prepareProcessor);
  }

  void sendView(NetView view, List<InternalDistributedMember> newMembers)
      throws InterruptedException {
    if (isShuttingDown()) {
      throw new InterruptedException("shutting down");
    }
    sendView(view, false, this.viewProcessor);
  }

  private boolean sendView(NetView view, boolean preparing, ViewReplyProcessor viewReplyProcessor)
      throws InterruptedException {

    int id = view.getViewId();
    InstallViewMessage msg = new InstallViewMessage(view,
        services.getAuthenticator().getCredentials(this.localAddress), preparing);
    Set<InternalDistributedMember> recips = new HashSet<>(view.getMembers());

    // a recent member was seen not to receive a new view - I think this is why
    // recips.removeAll(newMembers); // new members get the view in a JoinResponseMessage
    recips.remove(this.localAddress); // no need to send it to ourselves

    Set<InternalDistributedMember> responders = recips;
    if (!view.getCrashedMembers().isEmpty()) {
      recips = new HashSet<>(recips);
      recips.addAll(view.getCrashedMembers());
    }

    if (preparing) {
      this.preparedView = view;
    } else {
      // Added a check in the view processor to turn off the ViewCreator
      // if another server is the coordinator - GEODE-870
      ViewCreator thread = this.viewCreator;
      if (isCoordinator && !localAddress.equals(view.getCoordinator())
          && !localAddress.equals(view.getCreator())
          && thread != null) {
        thread.markViewCreatorForShutdown(view.getCoordinator());
        this.isCoordinator = false;
      }
      installView(new NetView(view, view.getViewId()));
    }

    if (recips.isEmpty()) {
      if (!preparing) {
        logger.info("no recipients for new view aside from myself");
      }
      return true;
    }

    logger.info((preparing ? "preparing" : "sending") + " new view " + view);

    msg.setRecipients(recips);

    Set<InternalDistributedMember> pendingLeaves = getPendingRequestIDs(LEAVE_REQUEST_MESSAGE);
    Set<InternalDistributedMember> pendingRemovals = getPendingRequestIDs(REMOVE_MEMBER_REQUEST);
    pendingRemovals.removeAll(view.getCrashedMembers());
    viewReplyProcessor.initialize(id, responders);
    viewReplyProcessor.processPendingRequests(pendingLeaves, pendingRemovals);
    addPublicKeysToView(view);
    services.getMessenger().send(msg, view);

    // only wait for responses during preparation
    if (preparing) {
      logger.debug("waiting for view responses");

      Set<InternalDistributedMember> failedToRespond = viewReplyProcessor.waitForResponses();

      logger.info("finished waiting for responses to view preparation");

      InternalDistributedMember conflictingViewSender =
          viewReplyProcessor.getConflictingViewSender();
      NetView conflictingView = viewReplyProcessor.getConflictingView();
      if (conflictingView != null) {
        logger.warn("received a conflicting membership view from " + conflictingViewSender
            + " during preparation: " + conflictingView);
        return false;
      }

      if (!failedToRespond.isEmpty() && (!services.getCancelCriterion().isCancelInProgress())) {
        logger.warn("these members failed to respond to the view change: " + failedToRespond);
        return false;
      }
    }

    return true;
  }

  private void addPublicKeysToView(NetView view) {
    String sDHAlgo = services.getConfig().getDistributionConfig().getSecurityUDPDHAlgo();
    if (sDHAlgo != null && !sDHAlgo.isEmpty()) {
      for (InternalDistributedMember mbr : view.getMembers()) {
        if (Objects.isNull(view.getPublicKey(mbr))) {
          byte[] pk = services.getMessenger().getPublicKey(mbr);
          view.setPublicKey(mbr, pk);
        }
      }
    }
  }

  void processMessage(final InstallViewMessage m) {
    if (isStopping) {
      return;
    }

    logger.debug("processing membership view message {}", m);

    NetView view = m.getView();

    // If our current view doesn't contaion sender then we wanrt to ignore that view.
    if (currentView != null && !currentView.contains(m.getSender())) {
      // but if preparedView contains sender then we don't want to ignore that view.
      // this may happen when we locator re-join and it take over coordinator's responsibility.
      if (this.preparedView == null || !this.preparedView.contains(m.getSender())) {
        logger.info("Ignoring the view {} from member {}, which is not in my current view {} ",
            view, m.getSender(), currentView);
        return;
      }
    }

    if (currentView != null && view.getViewId() < currentView.getViewId()) {
      // ignore old views
      ackView(m);
      return;
    }

    boolean viewContainsMyNewAddress = false;
    if (!this.isJoined && !m.isPreparing()) {
      // if we're still waiting for a join response and we're in this view we
      // should install the view so join() can finish its work
      for (InternalDistributedMember mbr : view.getMembers()) {
        if (localAddress.equals(mbr)) {
          viewContainsMyNewAddress = true;
          break;
        }
      }
    }

    if (m.isPreparing()) {
      if (this.preparedView != null && this.preparedView.getViewId() >= view.getViewId()) {
        if (this.preparedView.getViewId() == view.getViewId() &&
            this.preparedView.getCreator().equals(view.getCreator())) {
          // this can happen if we received two prepares during auto-reconnect
        } else {
          // send the conflicting view to the creator of this new view
          services.getMessenger()
              .send(new ViewAckMessage(view.getViewId(), m.getSender(), this.preparedView));
        }
      } else {
        this.preparedView = view;
        // complete filling in the member ID of this node, if possible
        for (InternalDistributedMember mbr : view.getMembers()) {
          if (this.localAddress.equals(mbr)) {
            this.birthViewId = mbr.getVmViewId();
            this.localAddress.setVmViewId(this.birthViewId);
            GMSMember me = (GMSMember) this.localAddress.getNetMember();
            me.setBirthViewId(birthViewId);
            break;
          }
        }
        ackView(m);
      }
    } else { // !preparing
      if (isJoined && currentView != null && !view.contains(this.localAddress)) {
        logger.fatal(
            "This member is no longer in the membership view.  My ID is {} and the new view is {}",
            localAddress, view);
        forceDisconnect("This node is no longer in the membership view");
      } else {
        if (isJoined || viewContainsMyNewAddress) {
          installView(view);
        }
        if (!m.isRebroadcast()) { // no need to ack a rebroadcast view
          ackView(m);
        }
      }
    }
  }

  void forceDisconnect(String reason) {
    this.isStopping = true;
    if (!isJoined) {
      joinResponse[0] =
          new JoinResponseMessage(
              "Stopping due to ForcedDisconnectException caused by '" + reason + "'", -1);
      isJoined = false;
      synchronized (joinResponse) {
        joinResponse.notifyAll();
      }
    } else {
      services.getManager().forceDisconnect(reason);
    }
  }

  private void ackView(InstallViewMessage m) {
    if (!playingDead && m.getView().contains(m.getView().getCreator())) {
      services.getMessenger()
          .send(new ViewAckMessage(m.getSender(), m.getView().getViewId(), m.isPreparing()));
    }
  }

  void processMessage(ViewAckMessage m) {
    if (isStopping) {
      return;
    }

    if (m.isPrepareAck()) {
      this.prepareProcessor.processViewResponse(m.getViewId(), m.getSender(), m.getAlternateView());
    } else {
      this.viewProcessor.processViewResponse(m.getViewId(), m.getSender(), m.getAlternateView());
    }
  }

  private TcpClientWrapper tcpClientWrapper = new TcpClientWrapper();

  /***
   * testing purpose. Sets the TcpClient that is used by GMSJoinLeave to communicate with Locators.
   *
   * @param tcpClientWrapper the wrapper
   */
  void setTcpClientWrapper(TcpClientWrapper tcpClientWrapper) {
    this.tcpClientWrapper = tcpClientWrapper;
  }

  /**
   * This contacts the locators to find out who the current coordinator is. All locators are
   * contacted. If they don't agree then we choose the oldest coordinator and return it.
   */
  boolean findCoordinator() {
    SearchState state = searchState;

    assert this.localAddress != null;

    if (!state.hasContactedAJoinedLocator && state.registrants.size() >= locators.size()
        && state.view != null && state.viewId > state.lastFindCoordinatorInViewId) {
      state.lastFindCoordinatorInViewId = state.viewId;
      logger.info("using findCoordinatorFromView");
      return findCoordinatorFromView();
    }

    String dhalgo = services.getConfig().getDistributionConfig().getSecurityUDPDHAlgo();
    FindCoordinatorRequest request = new FindCoordinatorRequest(this.localAddress,
        state.alreadyTried, state.viewId, services.getMessenger().getPublicKey(localAddress),
        services.getMessenger().getRequestId(), dhalgo);
    Set<InternalDistributedMember> possibleCoordinators = new HashSet<InternalDistributedMember>();
    Set<InternalDistributedMember> coordinatorsWithView = new HashSet<InternalDistributedMember>();

    long giveUpTime =
        System.currentTimeMillis() + ((long) services.getConfig().getLocatorWaitTime() * 1000L);

    int connectTimeout = (int) services.getConfig().getMemberTimeout() * 2;
    boolean anyResponses = false;

    logger.debug("sending {} to {}", request, locators);

    state.hasContactedAJoinedLocator = false;
    state.locatorsContacted = 0;

    do {
      for (HostAddress laddr : locators) {
        try {
          InetSocketAddress addr = laddr.getSocketInetAddress();
          Object o = tcpClientWrapper.sendCoordinatorFindRequest(addr, request, connectTimeout);
          FindCoordinatorResponse response =
              (o instanceof FindCoordinatorResponse) ? (FindCoordinatorResponse) o : null;
          if (response != null) {
            if (response.getRejectionMessage() != null) {
              throw new GemFireConfigException(response.getRejectionMessage());
            }
            setCoordinatorPublicKey(response);
            state.locatorsContacted++;
            if (response.getRegistrants() != null) {
              state.registrants.addAll(response.getRegistrants());
            }
            logger.info("received {}", response);
            if (!state.hasContactedAJoinedLocator && response.getSenderId() != null
                && response.getSenderId().getVmViewId() >= 0) {
              logger.info("Locator's address indicates it is part of a distributed system "
                  + "so I will not become membership coordinator on this attempt to join");
              state.hasContactedAJoinedLocator = true;
            }
            InternalDistributedMember responseCoordinator = response.getCoordinator();
            if (responseCoordinator != null) {
              anyResponses = true;
              NetView v = response.getView();
              int viewId = v == null ? -1 : v.getViewId();
              if (viewId > state.viewId) {
                state.viewId = viewId;
                state.view = v;
                state.registrants.clear();
              }
              if (viewId > -1) {
                coordinatorsWithView.add(responseCoordinator);
              }
              // if this node is restarting it should never create its own cluster because
              // the QuorumChecker would have contacted a quorum of live nodes and one of
              // them should already be the coordinator, or should become the coordinator soon
              boolean isMyOldAddress =
                  services.getConfig().isReconnecting() && localAddress.equals(responseCoordinator)
                      && responseCoordinator.getVmViewId() >= 0;
              if (!isMyOldAddress) {
                possibleCoordinators.add(response.getCoordinator());
              }
            }
          }
        } catch (IOException | ClassNotFoundException problem) {
          logger.debug("Exception thrown when contacting a locator", problem);
          if (state.locatorsContacted == 0 && System.currentTimeMillis() < giveUpTime) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              services.getCancelCriterion().checkCancelInProgress(e);
              throw new SystemConnectException("Interrupted while trying to contact locators");
            }
          }
        }
      }
    } while (!anyResponses && System.currentTimeMillis() < giveUpTime);
    if (possibleCoordinators.isEmpty()) {
      return false;
    }

    if (coordinatorsWithView.size() > 0) {
      possibleCoordinators = coordinatorsWithView;// lets check current coordinators in view only
    }

    Iterator<InternalDistributedMember> it = possibleCoordinators.iterator();
    if (possibleCoordinators.size() == 1) {
      state.possibleCoordinator = it.next();
    } else {
      InternalDistributedMember oldest = it.next();
      while (it.hasNext()) {
        InternalDistributedMember candidate = it.next();
        if (oldest.compareTo(candidate) > 0) {
          oldest = candidate;
        }
      }
      state.possibleCoordinator = oldest;
    }
    InternalDistributedMember coord = null;
    boolean coordIsNoob = true;
    for (; it.hasNext();) {
      InternalDistributedMember mbr = it.next();
      if (!state.alreadyTried.contains(mbr)) {
        boolean mbrIsNoob = (mbr.getVmViewId() < 0);
        if (mbrIsNoob) {
          // member has not yet joined
          if (coordIsNoob && (coord == null || coord.compareTo(mbr) > 0)) {
            coord = mbr;
          }
        } else {
          // member has already joined
          if (coordIsNoob || mbr.getVmViewId() > coord.getVmViewId()) {
            coord = mbr;
            coordIsNoob = false;
          }
        }
      }
    }
    logger.info("findCoordinator chose {} out of these possible coordinators: {}",
        state.possibleCoordinator, possibleCoordinators);
    return true;
  }

  protected class TcpClientWrapper {
    protected Object sendCoordinatorFindRequest(InetSocketAddress addr,
        FindCoordinatorRequest request, int connectTimeout)
        throws ClassNotFoundException, IOException {
      TcpClient client = new TcpClient();
      return client.requestToServer(addr, request, connectTimeout, true);
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WA_NOT_IN_LOOP")
  boolean findCoordinatorFromView() {
    ArrayList<FindCoordinatorResponse> result;
    SearchState state = searchState;
    NetView v = state.view;
    List<InternalDistributedMember> recipients = new ArrayList<>(v.getMembers());

    logger.info("searching for coordinator in findCoordinatorFromView");

    if (recipients.size() > MAX_DISCOVERY_NODES && MAX_DISCOVERY_NODES > 0) {
      recipients = recipients.subList(0, MAX_DISCOVERY_NODES);
    }
    if (state.registrants != null) {
      recipients.addAll(state.registrants);
    }
    recipients.remove(localAddress);

    logger.info("sending FindCoordinatorRequests to {}", recipients);

    boolean testing = unitTesting.contains("findCoordinatorFromView");
    synchronized (state.responses) {
      if (!testing) {
        state.responses.clear();
      }

      String dhalgo = services.getConfig().getDistributionConfig().getSecurityUDPDHAlgo();
      if (!dhalgo.isEmpty()) {
        // Here we are sending message one-by-one to all recipients as we don't have cluster secret
        // key yet.
        // Usually this happens when locator re-joins the cluster and it has saved view.
        for (InternalDistributedMember mbr : recipients) {
          Set<InternalDistributedMember> r = new HashSet<>();
          r.add(mbr);
          FindCoordinatorRequest req = new FindCoordinatorRequest(localAddress, state.alreadyTried,
              state.viewId, services.getMessenger().getPublicKey(localAddress),
              services.getMessenger().getRequestId(), dhalgo);
          req.setRecipients(r);

          services.getMessenger().send(req, v);
        }
      } else {
        FindCoordinatorRequest req = new FindCoordinatorRequest(localAddress, state.alreadyTried,
            state.viewId, services.getMessenger().getPublicKey(localAddress),
            services.getMessenger().getRequestId(), dhalgo);
        req.setRecipients(recipients);

        services.getMessenger().send(req, v);
      }
      try {
        if (!testing) {
          state.responsesExpected = recipients.size();
          state.responses.wait(DISCOVERY_TIMEOUT);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      result = new ArrayList<>(state.responses);
      state.responses.clear();
    }

    InternalDistributedMember bestGuessCoordinator = null;
    if (localAddress.getNetMember().preferredForCoordinator()) {
      // it's possible that all other potential coordinators are gone
      // and this new member must become the coordinator
      bestGuessCoordinator = localAddress;
    }
    state.joinedMembersContacted = 0;
    boolean bestGuessIsNotMember = true;
    for (FindCoordinatorResponse resp : result) {
      logger.info("findCoordinatorFromView processing {}", resp);
      InternalDistributedMember suggestedCoordinator = resp.getCoordinator();
      if (resp.getSenderId().getVmViewId() >= 0) {
        state.joinedMembersContacted++;
      }
      if (!localAddress.equals(suggestedCoordinator)
          && !state.alreadyTried.contains(suggestedCoordinator)) {
        boolean suggestedIsNotMember = (suggestedCoordinator.getVmViewId() < 0);
        if (suggestedIsNotMember) {
          // member has not yet joined
          if (bestGuessIsNotMember && (bestGuessCoordinator == null
              || bestGuessCoordinator.compareTo(suggestedCoordinator, false) > 0)) {
            bestGuessCoordinator = suggestedCoordinator;
          }
        } else {
          // member has already joined
          if (bestGuessIsNotMember
              || suggestedCoordinator.getVmViewId() > bestGuessCoordinator.getVmViewId()) {
            bestGuessCoordinator = suggestedCoordinator;
            bestGuessIsNotMember = false;
          }
        }
        logger.info("findCoordinatorFromView's best guess is now {}", bestGuessCoordinator);
      }
    }

    state.possibleCoordinator = bestGuessCoordinator;
    return bestGuessCoordinator != null;
  }

  /**
   * receives a JoinResponse holding a membership view or rejection message
   *
   * @param rsp the response message to process
   */
  void processMessage(JoinResponseMessage rsp) {
    if (isStopping) {
      return;
    }

    synchronized (joinResponse) {
      if (!this.isJoined) {
        // 1. our joinRequest rejected.
        // 2. Member which was coordinator but just now some other member became coordinator
        // 3. we got message with secret key, but still view is coming and that will inform the
        // joining thread
        if (rsp.getRejectionMessage() != null) {
          joinResponse[0] = rsp;
          joinResponse.notifyAll();
        } else if (rsp.getCurrentView() != null) {
          // ignore - we get to join when we receive a view. Joining earlier may
          // confuse other members if we've reused an old address
        } else {
          // we got secret key lets add it
          services.getMessenger().setClusterSecretKey(rsp.getSecretPk());
        }
      }
    }
  }

  /**
   * for testing, do not use in any other case as it is not thread safe
   */
  JoinResponseMessage[] getJoinResponseMessage() {
    return joinResponse;
  }

  /***
   * for testing purpose
   *
   * @param jrm the join response message to process
   */
  void setJoinResponseMessage(JoinResponseMessage jrm) {
    joinResponse[0] = jrm;
  }

  void processMessage(FindCoordinatorRequest req) {
    if (isStopping) {
      return;
    }

    FindCoordinatorResponse resp;
    if (this.isJoined) {
      NetView v = currentView;
      resp = new FindCoordinatorResponse(v.getCoordinator(), localAddress,
          services.getMessenger().getPublicKey(v.getCoordinator()), req.getRequestId());
    } else {
      resp = new FindCoordinatorResponse(localAddress, localAddress,
          services.getMessenger().getPublicKey(localAddress), req.getRequestId());
    }
    resp.setRecipient(req.getMemberID());
    services.getMessenger().send(resp);
  }

  void processMessage(FindCoordinatorResponse resp) {
    if (isStopping) {
      return;
    }

    synchronized (searchState.responses) {
      searchState.responses.add(resp);
      if (searchState.responsesExpected <= searchState.responses.size()) {
        searchState.responses.notifyAll();
      }
    }
    setCoordinatorPublicKey(resp);
  }

  private void setCoordinatorPublicKey(FindCoordinatorResponse response) {
    if (response.getCoordinator() != null && response.getCoordinatorPublicKey() != null)
      services.getMessenger().setPublicKey(response.getCoordinatorPublicKey(),
          response.getCoordinator());
  }

  void processMessage(NetworkPartitionMessage msg) {
    if (isStopping) {
      return;
    }

    String str = "Membership coordinator " + msg.getSender()
        + " has declared that a network partition has occurred";
    forceDisconnect(str);
  }

  @Override
  public NetView getView() {
    return currentView;
  }

  @Override
  public NetView getPreviousView() {
    return previousView;
  }

  @Override
  public InternalDistributedMember getMemberID() {
    return this.localAddress;
  }

  @Override
  public void installView(NetView newView) {

    synchronized (viewInstallationLock) {
      if (currentView != null && currentView.getViewId() >= newView.getViewId()) {
        // old view - ignore it
        return;
      }

      logger.info("received new view: {}\nold view is: {}", newView, currentView);

      if (currentView == null && !this.isJoined) {
        boolean found = false;
        for (InternalDistributedMember mbr : newView.getMembers()) {
          if (this.localAddress.equals(mbr)) {
            found = true;
            this.birthViewId = mbr.getVmViewId();
            this.localAddress.setVmViewId(this.birthViewId);
            GMSMember me = (GMSMember) this.localAddress.getNetMember();
            me.setBirthViewId(birthViewId);
            break;
          }
        }
        if (!found) {
          logger.info("rejecting view (not yet joined)");
          return;
        }
      }

      if (isJoined && isNetworkPartition(newView, true)) {
        if (quorumRequired) {
          Set<InternalDistributedMember> crashes = newView.getActualCrashedMembers(currentView);
          forceDisconnect(String.format(
              "Exiting due to possible network partition event due to loss of %s cache processes: %s",
              crashes.size(), crashes));
          return;
        }
      }

      previousView = currentView;
      currentView = newView;
      preparedView = null;
      if (viewCreator != null)
        viewCreator.setLastConflictingView(null);
      services.installView(newView);

      if (!isJoined) {
        logger.debug("notifying join thread");
        isJoined = true;
        synchronized (joinResponse) {
          joinResponse.notifyAll();
        }
      }

      if (!newView.getCreator().equals(this.localAddress)) {
        NetView check = new NetView(newView, newView.getViewId() + 1);
        synchronized (leftMembers) {
          check.removeAll(leftMembers);
        }
        synchronized (removedMembers) {
          check.removeAll(removedMembers);
          check.addCrashedMembers(removedMembers);
        }
        if (check.shouldBeCoordinator(this.localAddress)) {
          if (!isCoordinator) {
            becomeCoordinator();
          }
        } else if (this.isCoordinator) {
          // stop being coordinator
          stopCoordinatorServices();
          this.isCoordinator = false;
        }
      }
      // TODO: remove this if block entirely according to Bruce
      if (!this.isCoordinator) {
        // get rid of outdated requests. It's possible some requests are
        // newer than the view just processed - the senders will have to
        // resend these
        synchronized (viewRequests) {
          for (Iterator<DistributionMessage> it = viewRequests.iterator(); it.hasNext();) {
            DistributionMessage m = it.next();
            if (m instanceof JoinRequestMessage) {
              if (currentView.contains(((JoinRequestMessage) m).getMemberID())) {
                it.remove();
              }
            } else if (m instanceof LeaveRequestMessage) {
              if (!currentView.contains(((LeaveRequestMessage) m).getMemberID())) {
                it.remove();
              }
            } else if (m instanceof RemoveMemberMessage) {
              if (!currentView.contains(((RemoveMemberMessage) m).getMemberID())) {
                it.remove();
              }
            }
          }
        }
      }
    }
    synchronized (removedMembers) {
      removeMembersFromCollectionIfNotInView(removedMembers, currentView);
    }
    synchronized (leftMembers) {
      removeMembersFromCollectionIfNotInView(leftMembers, currentView);
    }
  }

  private void removeMembersFromCollectionIfNotInView(Collection<InternalDistributedMember> members,
      NetView currentView) {
    Iterator<InternalDistributedMember> iterator = members.iterator();
    while (iterator.hasNext()) {
      if (!currentView.contains(iterator.next())) {
        iterator.remove();
      }
    }
  }

  /**
   * Sends a message declaring a network partition to the members of the given view via Messenger
   *
   */
  void sendNetworkPartitionMessage(NetView view) {
    List<InternalDistributedMember> recipients = new ArrayList<>(view.getMembers());
    recipients.remove(localAddress);
    NetworkPartitionMessage msg = new NetworkPartitionMessage(recipients);
    try {
      services.getMessenger().send(msg);
    } catch (RuntimeException e) {
      logger.debug("unable to send network partition message - continuing", e);
    }
  }

  /**
   * returns true if this member thinks it is the membership coordinator for the distributed system
   */
  public boolean isCoordinator() {
    return this.isCoordinator;
  }

  /**
   * return true if we're stopping or are stopped
   */
  public boolean isStopping() {
    return this.isStopping;
  }

  /**
   * returns the currently prepared view, if any
   */
  public NetView getPreparedView() {
    return this.preparedView;
  }

  /**
   * check to see if the new view shows a drop of 51% or more
   */
  boolean isNetworkPartition(NetView newView, boolean logWeights) {
    if (currentView == null) {
      return false;
    }
    int oldWeight = currentView.memberWeight();
    int failedWeight = newView.getCrashedMemberWeight(currentView);
    if (failedWeight > 0 && logWeights) {
      if (logger.isInfoEnabled() && newView.getCreator().equals(localAddress)) { // view-creator
                                                                                 // logs this
        newView.logCrashedMemberWeights(currentView, logger);
      }
      int failurePoint = (int) (Math.round(51.0 * oldWeight) / 100.0);
      if (failedWeight > failurePoint && quorumLostView != newView) {
        quorumLostView = newView;
        logger.warn("total weight lost in this view change is {} of {}.  Quorum has been lost!",
            failedWeight, oldWeight);
        services.getManager().quorumLost(newView.getActualCrashedMembers(currentView), currentView);
        return true;
      }
    }
    return false;
  }

  private void stopCoordinatorServices() {
    if (viewCreator != null && !viewCreator.isShutdown()) {
      logger.debug("Shutting down ViewCreator");
      viewCreator.shutdown();
      try {
        viewCreator.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void startViewBroadcaster() {
    services.getTimer().schedule(new ViewBroadcaster(), VIEW_BROADCAST_INTERVAL,
        VIEW_BROADCAST_INTERVAL);
  }

  public static void loadEmergencyClasses() {}

  @Override
  public void emergencyClose() {
    isStopping = true;
    isJoined = false;
    stopCoordinatorServices();
    isCoordinator = false;
  }

  @Override
  public void beSick() {}

  @Override
  public void playDead() {
    playingDead = true;
  }

  @Override
  public void beHealthy() {
    playingDead = false;
  }

  @Override
  public void start() {}

  @Override
  public void started() {}

  public void setLocalAddress(InternalDistributedMember address) {
    this.localAddress = address;
    GMSMember mbr = (GMSMember) this.localAddress.getNetMember();

    if (services.getConfig().areLocatorsPreferredAsCoordinators()) {
      boolean preferred = false;
      if (services.getLocator() != null || Locator.hasLocator()
          || !services.getConfig().getDistributionConfig().getStartLocator().isEmpty()
          || localAddress.getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE) {
        logger
            .info("This member is hosting a locator will be preferred as a membership coordinator");
        preferred = true;
      }
      mbr.setPreferredForCoordinator(preferred);
    } else {
      mbr.setPreferredForCoordinator(true);
    }
  }

  @Override
  public void stop() {
    logger.debug("JoinLeave stopping");
    leave();
  }

  @Override
  public void stopped() {}

  @Override
  public void memberSuspected(InternalDistributedMember initiator,
      InternalDistributedMember suspect, String reason) {
    prepareProcessor.memberSuspected(suspect);
    viewProcessor.memberSuspected(suspect);
  }

  @Override
  public void leave() {
    synchronized (viewInstallationLock) {
      NetView view = currentView;
      isStopping = true;
      stopCoordinatorServices();
      if (view != null) {
        if (view.size() > 1) {
          List<InternalDistributedMember> coords =
              view.getPreferredCoordinators(Collections.emptySet(), localAddress, 5);
          logger.debug("Sending my leave request to {}", coords);
          LeaveRequestMessage m =
              new LeaveRequestMessage(coords, this.localAddress, "this member is shutting down");
          services.getMessenger().send(m);
        } // view.size
      } // view != null
    }
  }

  @Override
  public void remove(InternalDistributedMember m, String reason) {
    NetView v = this.currentView;

    services.getCancelCriterion().checkCancelInProgress(null);

    if (v != null && v.contains(m)) {
      Set<InternalDistributedMember> filter = new HashSet<>();
      filter.add(m);
      RemoveMemberMessage msg =
          new RemoveMemberMessage(v.getPreferredCoordinators(filter, getMemberID(), 5), m, reason);
      msg.setSender(this.localAddress);
      processMessage(msg);
      if (!this.isCoordinator) {
        msg.resetRecipients();
        msg.setRecipients(v.getPreferredCoordinators(Collections.emptySet(), localAddress,
            ServiceConfig.SMALL_CLUSTER_SIZE + 1));
        services.getMessenger().send(msg);
      }
    } else {
      RemoveMemberMessage msg = new RemoveMemberMessage(m, m, reason);
      services.getMessenger().send(msg);
    }
  }

  @Override
  public void memberShutdown(DistributedMember mbr, String reason) {
    LeaveRequestMessage msg = new LeaveRequestMessage(Collections.singleton(this.localAddress),
        (InternalDistributedMember) mbr, reason);
    msg.setSender((InternalDistributedMember) mbr);
    processMessage(msg);
  }

  boolean checkIfAvailable(InternalDistributedMember fmbr) {
    // return the member id if it fails health checks
    logger.info("checking state of member " + fmbr);
    if (services.getHealthMonitor().checkIfAvailable(fmbr,
        "Member failed to acknowledge a membership view", false)) {
      logger.info("member " + fmbr + " passed availability check");
      return true;
    }
    logger.info("member " + fmbr + " failed availability check");
    return false;
  }

  private InternalDistributedMember getMemId(NetMember jgId,
      List<InternalDistributedMember> members) {
    for (InternalDistributedMember m : members) {
      if (m.getNetMember().equals(jgId)) {
        return m;
      }
    }
    return null;
  }

  @Override
  public InternalDistributedMember getMemberID(NetMember jgId) {
    NetView v = currentView;
    InternalDistributedMember ret = null;
    if (v != null) {
      ret = getMemId(jgId, v.getMembers());
    }

    if (ret == null) {
      v = preparedView;
      if (v != null) {
        ret = getMemId(jgId, v.getMembers());
      }
    }

    if (ret == null) {
      return new InternalDistributedMember(jgId);
    }

    return ret;
  }

  @Override
  public void disableDisconnectOnQuorumLossForTesting() {
    this.quorumRequired = false;
  }

  @Override
  public void init(Services s) {
    this.services = s;

    DistributionConfig dc = services.getConfig().getDistributionConfig();
    if (dc.getMcastPort() != 0 && StringUtils.isBlank(dc.getLocators())
        && StringUtils.isBlank(dc.getStartLocator())) {
      throw new GemFireConfigException("Multicast cannot be configured for a non-distributed cache."
          + "  Please configure the locator services for this cache using " + LOCATORS + " or "
          + START_LOCATOR + ".");
    }

    services.getMessenger().addHandler(JoinRequestMessage.class, this::processMessage);
    services.getMessenger().addHandler(JoinResponseMessage.class, this::processMessage);
    services.getMessenger().addHandler(InstallViewMessage.class, this::processMessage);
    services.getMessenger().addHandler(ViewAckMessage.class, this::processMessage);
    services.getMessenger().addHandler(LeaveRequestMessage.class, this::processMessage);
    services.getMessenger().addHandler(RemoveMemberMessage.class, this::processMessage);
    services.getMessenger().addHandler(FindCoordinatorRequest.class,
        this::processMessage);
    services.getMessenger().addHandler(FindCoordinatorResponse.class,
        this::processMessage);
    services.getMessenger().addHandler(NetworkPartitionMessage.class,
        this::processMessage);

    int ackCollectionTimeout = dc.getMemberTimeout() * 2 * 12437 / 10000;
    if (ackCollectionTimeout < 1500) {
      ackCollectionTimeout = 1500;
    } else if (ackCollectionTimeout > 12437) {
      ackCollectionTimeout = 12437;
    }
    ackCollectionTimeout = Integer
        .getInteger(DistributionConfig.GEMFIRE_PREFIX + "VIEW_ACK_TIMEOUT", ackCollectionTimeout)
        .intValue();
    this.viewAckTimeout = ackCollectionTimeout;

    this.quorumRequired =
        services.getConfig().getDistributionConfig().getEnableNetworkPartitionDetection();

    DistributionConfig dconfig = services.getConfig().getDistributionConfig();
    String bindAddr = dconfig.getBindAddress();
    locators = GMSUtil.parseLocators(dconfig.getLocators(), bindAddr);
    if (logger.isDebugEnabled()) {
      logger.debug("Parsed locators are {}", locators);
    }
  }

  /**
   * returns the member IDs of the pending requests having the given DataSerializableFixedID
   */
  Set<InternalDistributedMember> getPendingRequestIDs(int theDSFID) {
    Set<InternalDistributedMember> result = new HashSet<>();
    synchronized (viewRequests) {
      for (DistributionMessage msg : viewRequests) {
        if (msg.getDSFID() == theDSFID) {
          result.add(((HasMemberID) msg).getMemberID());
        }
      }
    }
    return result;
  }

  /***
   * test method
   *
   */
  protected ViewReplyProcessor getPrepareViewReplyProcessor() {
    return prepareProcessor;
  }

  protected boolean testPrepareProcessorWaiting() {
    return prepareProcessor.isWaiting();
  }

  class ViewReplyProcessor {
    volatile int viewId = -1;
    final Set<InternalDistributedMember> notRepliedYet = new HashSet<>();
    NetView conflictingView;
    InternalDistributedMember conflictingViewSender;
    volatile boolean waiting;
    final boolean isPrepareViewProcessor;
    final Set<InternalDistributedMember> pendingRemovals = new HashSet<>();

    ViewReplyProcessor(boolean forPreparation) {
      this.isPrepareViewProcessor = forPreparation;
    }

    synchronized void initialize(int viewId, Set<InternalDistributedMember> recips) {
      waiting = true;
      this.viewId = viewId;
      notRepliedYet.clear();
      notRepliedYet.addAll(recips);
      conflictingView = null;
      pendingRemovals.clear();
    }

    boolean isWaiting() {
      return waiting;
    }

    synchronized void processPendingRequests(Set<InternalDistributedMember> pendingLeaves,
        Set<InternalDistributedMember> pendingRemovals) {
      // there's no point in waiting for members who have already
      // requested to leave or who have been declared crashed.
      // We don't want to mix the two because pending removals
      // aren't reflected as having crashed in the current view
      // and need to cause a new view to be generated
      for (InternalDistributedMember mbr : pendingLeaves) {
        notRepliedYet.remove(mbr);
      }
      for (InternalDistributedMember mbr : pendingRemovals) {
        if (this.notRepliedYet.contains(mbr)) {
          this.pendingRemovals.add(mbr);
        }
      }
    }

    synchronized void memberSuspected(InternalDistributedMember suspect) {
      if (waiting) {
        // we will do a final check on this member if it hasn't already
        // been done, so stop waiting for it now
        logger.debug("view response processor recording suspect status for {}", suspect);
        if (notRepliedYet.contains(suspect) && !pendingRemovals.contains(suspect)) {
          pendingRemovals.add(suspect);
          checkIfDone();
        }
      }
    }

    synchronized void processLeaveRequest(InternalDistributedMember mbr) {
      if (waiting) {
        logger.debug("view response processor recording leave request for {}", mbr);
        stopWaitingFor(mbr);
      }
    }

    synchronized void processRemoveRequest(InternalDistributedMember mbr) {
      if (waiting) {
        logger.debug("view response processor recording remove request for {}", mbr);
        pendingRemovals.add(mbr);
        checkIfDone();
      }
    }

    synchronized void processViewResponse(int viewId, InternalDistributedMember sender,
        NetView conflictingView) {
      if (!waiting) {
        return;
      }

      if (viewId == this.viewId) {
        if (conflictingView != null) {
          this.conflictingViewSender = sender;
          this.conflictingView = conflictingView;
        }

        logger.debug("view response processor recording response for {}", sender);
        stopWaitingFor(sender);
      }
    }

    /**
     * call with synchronized(this)
     */
    private void stopWaitingFor(InternalDistributedMember mbr) {
      notRepliedYet.remove(mbr);
      checkIfDone();
    }

    /**
     * call with synchronized(this)
     */
    private void checkIfDone() {
      if (notRepliedYet.isEmpty()
          || (pendingRemovals != null && pendingRemovals.containsAll(notRepliedYet))) {
        logger.debug("All anticipated view responses received - notifying waiting thread");
        waiting = false;
        notifyAll();
      } else {
        logger.debug("Still waiting for these view replies: {}", notRepliedYet);
      }
    }

    Set<InternalDistributedMember> waitForResponses() throws InterruptedException {
      Set<InternalDistributedMember> result;
      long endOfWait = System.currentTimeMillis() + viewAckTimeout;
      try {
        while (System.currentTimeMillis() < endOfWait
            && (!services.getCancelCriterion().isCancelInProgress())) {
          try {
            synchronized (this) {
              if (!waiting || this.notRepliedYet.isEmpty() || this.conflictingView != null) {
                break;
              }
              wait(1000);
            }
          } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for view responses");
            throw e;
          }
        }
      } finally {
        synchronized (this) {
          if (!this.waiting) {
            // if we've set waiting to false due to incoming messages then
            // we've discounted receiving any other responses from the
            // remaining members due to leave/crash notification
            result = new HashSet<>(pendingRemovals);
          } else {
            result = new HashSet<>(this.notRepliedYet);
            result.addAll(pendingRemovals);
            this.waiting = false;
          }
        }
      }
      return result;
    }

    NetView getConflictingView() {
      return this.conflictingView;
    }

    InternalDistributedMember getConflictingViewSender() {
      return this.conflictingViewSender;
    }

    synchronized Set<InternalDistributedMember> getUnresponsiveMembers() {
      return new HashSet<>(this.notRepliedYet);
    }
  }

  /**
   * ViewBroadcaster periodically sends the current view to all current and departed members. This
   * ensures that a member that missed the view will eventually see it and act on it.
   */
  class ViewBroadcaster extends TimerTask {

    @Override
    public void run() {
      if (!isCoordinator || isStopping) {
        cancel();
      } else {
        sendCurrentView();
      }
    }

    void sendCurrentView() {
      NetView v = currentView;
      if (v != null) {
        InstallViewMessage msg = new InstallViewMessage(v,
            services.getAuthenticator().getCredentials(localAddress), false);
        Collection<InternalDistributedMember> recips =
            new ArrayList<>(v.size() + v.getCrashedMembers().size());
        recips.addAll(v.getMembers());
        recips.remove(localAddress);
        recips.addAll(v.getCrashedMembers());
        msg.setRecipients(recips);
        // use sendUnreliably since we are sending to crashed members &
        // don't want any retransmission tasks set up for them
        services.getMessenger().sendUnreliably(msg);
      }
    }
  }


  static class ViewAbandonedException extends Exception {
  }
}
