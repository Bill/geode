package org.apache.geode.distributed.internal.membership.gms.membership;

import static org.apache.geode.internal.DataSerializableFixedID.JOIN_REQUEST;
import static org.apache.geode.internal.DataSerializableFixedID.LEAVE_REQUEST_MESSAGE;
import static org.apache.geode.internal.DataSerializableFixedID.REMOVE_MEMBER_REQUEST;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.NetView;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Locator;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Messenger;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinResponseMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.logging.LoggingThread;

class ViewCreator extends LoggingThread {

  /**
   * time to wait for a broadcast message to be transmitted by jgroups
   */
  private static final long BROADCAST_MESSAGE_SLEEP_TIME =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "broadcast-message-sleep-time", 1000);

  /*
   Services
   */

  private final GMSJoinLeave gmsJoinLeave;
  private final Logger logger;
  private final Locator locator;
  private final Messenger messenger;
  private final InternalDistributedMember localAddress;

  /*
   Work queue
   */
  private final List<DistributionMessage> viewRequests;

  private final int viewAckTimeout;
  private final long requestCollectionInterval;
  private final long memberTimeout;

  /**
   * the last view that conflicted with view preparation
   */
  private volatile NetView lastConflictingView;

  private volatile boolean shutdown = false;
  volatile boolean waiting = false;
  private volatile boolean testFlagForRemovalRequest = false;
  // count of number of views abandoned due to conflicts
  private volatile int abandonedViews = 0;
  private volatile boolean markViewCreatorForShutdown = false; // see GEODE-870

  /**
   * initial view to install. guarded by synch on ViewCreator
   * GuardedBy("this")
   */
  NetView initialView;

  /**
   * initial joining members. guarded by synch on ViewCreator
   * GuardedBy("this")
   */
  private List<InternalDistributedMember> initialJoins = Collections.emptyList();

  /**
   * initial leaving members guarded by synch on ViewCreator
   * GuardedBy("this")
   */
  private Set<InternalDistributedMember> initialLeaving;

  /**
   * initial crashed members. guarded by synch on ViewCreator
   * GuardedBy("this")
   */
  private Set<InternalDistributedMember> initialRemovals;

  ViewCreator(final String name,
              final GMSJoinLeave gmsJoinLeave,
              final Logger logger,
              final List<DistributionMessage> viewRequests,
              final InternalDistributedMember localAddress,
              final int viewAckTimeout,
              final long requestCollectionInterval,
              final long memberTimeout,
              final Locator locator,
              final Messenger messenger) {
    super(name);
    this.gmsJoinLeave = gmsJoinLeave;
    this.logger = logger;
    this.viewRequests = viewRequests;
    this.localAddress = localAddress;
    this.viewAckTimeout = viewAckTimeout;
    this.requestCollectionInterval = requestCollectionInterval;
    this.lastConflictingView = null;

    this.memberTimeout = memberTimeout;
    this.locator = locator;
    this.messenger = messenger;

  }

  void setLastConflictingView(final NetView lastConflictingView) {
    this.lastConflictingView = lastConflictingView;
  }

  void shutdown() {
    setShutdownFlag();
    synchronized (viewRequests) {
      viewRequests.notifyAll();
      interrupt();
    }
  }

  boolean isShutdown() {
    return shutdown;
  }

  boolean isWaiting() {
    return waiting;
  }

  int getAbandonedViewCount() {
    return abandonedViews;
  }

  /**
   * All views should be sent by the ViewCreator thread, so if this member becomes coordinator it
   * may have an initial view to transmit that announces the removal of the former coordinator to
   *
   * @param leaving - members leaving in this view
   * @param removals - members crashed in this view
   */
  synchronized void setInitialView(NetView newView, List<InternalDistributedMember> newMembers,
      Set<InternalDistributedMember> leaving, Set<InternalDistributedMember> removals) {
    this.initialView = newView;
    this.initialJoins = newMembers;
    this.initialLeaving = leaving;
    this.initialRemovals = removals;
  }

  private void sendInitialView() {
    boolean retry;
    do {
      retry = false;
      try {
        if (initialView == null) {
          return;
        }
        NetView v = gmsJoinLeave.getPreparedView();
        if (v != null) {
          processPreparedView(v);
        }
        try {
          NetView iView;
          List<InternalDistributedMember> iJoins;
          Set<InternalDistributedMember> iLeaves;
          Set<InternalDistributedMember> iRemoves;
          synchronized (this) {
            iView = initialView;
            iJoins = initialJoins;
            iLeaves = initialLeaving;
            iRemoves = initialRemovals;
          }
          if (iView != null) {
            prepareAndSendView(iView, iJoins, iLeaves, iRemoves);
          }
        } finally {
          setInitialView(null, null, null, null);
        }
      } catch (GMSJoinLeave.ViewAbandonedException e) {
        // another view creator is active - sleep a bit to let it finish or go away
        retry = true;
        try {
          sleep(memberTimeout);
        } catch (InterruptedException e2) {
          setShutdownFlag();
          retry = false;
        }
      } catch (InterruptedException e) {
        setShutdownFlag();
      } catch (DistributedSystemDisconnectedException e) {
        setShutdownFlag();
      }
    } while (retry);
  }

  /**
   * marks this ViewCreator as being shut down. It may be some short amount of time before the
   * ViewCreator thread exits.
   */
  private void setShutdownFlag() {
    shutdown = true;
  }

  /**
   * This allows GMSJoinLeave to tell the ViewCreator to shut down after finishing its current
   * task. See GEODE-870.
   */
  void markViewCreatorForShutdown(InternalDistributedMember viewCreator) {
    logger.info(
        "Marking view creator for shutdown because {} is now the coordinator.  My address is {}."
            + "  Net member IDs are {} and {} respectively",
        viewCreator, localAddress, viewCreator.getNetMember(), localAddress.getNetMember());
    this.markViewCreatorForShutdown = true;
  }

  /**
   * During initial view processing a prepared view was discovered. This method will extract its
   * new members and create a new initial view containing them.
   *
   * @param v The prepared view
   */
  private void processPreparedView(NetView v) {
    assert initialView != null;
    final NetView currentView = gmsJoinLeave.getView();
    if (currentView == null || currentView.getViewId() < v.getViewId()) {
      // we have a prepared view that is newer than the current view
      // form a new View ID
      int viewId = Math.max(initialView.getViewId(), v.getViewId());
      viewId += 1;
      NetView newView = new NetView(initialView, viewId);

      // add the new members from the prepared view to the new view,
      // preserving their failure-detection ports
      List<InternalDistributedMember> newMembers;
      if (currentView != null) {
        newMembers = v.getNewMembers(currentView);
      } else {
        newMembers = v.getMembers();
      }
      for (InternalDistributedMember newMember : newMembers) {
        newView.add(newMember);
        newView.setFailureDetectionPort(newMember, v.getFailureDetectionPort(newMember));
        newView.setPublicKey(newMember, v.getPublicKey(newMember));
      }

      // use the new view as the initial view
      synchronized (this) {
        setInitialView(newView, newMembers, initialLeaving, initialRemovals);
      }
    }
  }

  @Override
  public void run() {
    List<DistributionMessage> requests = null;
    logger.info("View Creator thread is starting");
    sendInitialView();
    long okayToCreateView = System.currentTimeMillis() + requestCollectionInterval;
    try {
      for (;;) {
        synchronized (viewRequests) {
          if (shutdown) {
            return;
          }
          if (viewRequests.isEmpty()) {
            try {
              logger.debug("View Creator is waiting for requests");
              waiting = true;
              viewRequests.wait();
            } catch (InterruptedException e) {
              return;
            } finally {
              waiting = false;
            }
            if (shutdown || Thread.currentThread().isInterrupted()) {
              return;
            }
            if (viewRequests.size() == 1) {
              // start the timer when we have only one request because
              // concurrent startup / shutdown of multiple members is
              // a common occurrence
              okayToCreateView = System.currentTimeMillis() + requestCollectionInterval;
              continue;
            }
          } else {
            long timeRemaining = okayToCreateView - System.currentTimeMillis();
            if (timeRemaining > 0) {
              // sleep to let more requests arrive
              try {
                viewRequests.wait(Math.min(100, timeRemaining));
                continue;
              } catch (InterruptedException e) {
                return;
              }
            } else {
              // time to create a new membership view
              if (requests == null) {
                requests = new ArrayList<DistributionMessage>(viewRequests);
              } else {
                requests.addAll(viewRequests);
              }
              viewRequests.clear();
              okayToCreateView = System.currentTimeMillis() + requestCollectionInterval;
            }
          }
        } // synchronized
        if (requests != null && !requests.isEmpty()) {
          logger.info("View Creator is processing {} requests for the next membership view ({})",
              requests.size(), requests);
          try {
            createAndSendView(requests);
            if (shutdown) {
              return;
            }
          } catch (GMSJoinLeave.ViewAbandonedException e) {
            synchronized (viewRequests) {
              viewRequests.addAll(requests);
            }
            // pause before reattempting so that another view creator can either finish
            // or fail
            try {
              sleep(memberTimeout);
            } catch (InterruptedException e2) {
              setShutdownFlag();
            }
          } catch (DistributedSystemDisconnectedException e) {
            setShutdownFlag();
          } catch (InterruptedException e) {
            logger.info("View Creator thread interrupted");
            setShutdownFlag();
          }
          requests = null;
        }
      }
    } finally {
      logger.info("View Creator thread is exiting");
      setShutdownFlag();
      informToPendingJoinRequests();
      if (locator != null) {
        locator.setIsCoordinator(false);
      }
    }
  }

  synchronized boolean informToPendingJoinRequests() {

    if (!shutdown) {
      return false;
    }
    NetView v = gmsJoinLeave.getView();
    if (v.getCoordinator().equals(localAddress)) {
      return false;
    }

    ArrayList<JoinRequestMessage> requests = new ArrayList<>();
    synchronized (viewRequests) {
      if (viewRequests.isEmpty()) {
        return false;
      }
      for (Iterator<DistributionMessage> iterator = viewRequests.iterator(); iterator
          .hasNext();) {
        DistributionMessage msg = iterator.next();
        switch (msg.getDSFID()) {
          case JOIN_REQUEST:
            requests.add((JoinRequestMessage) msg);
            break;
          default:
            break;
        }
      }
    }

    if (requests.isEmpty()) {
      return false;
    }

    for (JoinRequestMessage msg : requests) {
      logger.debug("Sending coordinator to pending join request from {} myid {} coord {}",
          msg.getSender(), localAddress, v.getCoordinator());
      JoinResponseMessage jrm = new JoinResponseMessage(msg.getMemberID(), v, msg.getRequestId());
      messenger.send(jrm);
    }

    return true;
  }

  /**
   * Create a new membership view and send it to members (including crashed members). Returns
   * false if the view cannot be prepared successfully, true otherwise
   *
   */
  void createAndSendView(List<DistributionMessage> requests)
      throws InterruptedException, GMSJoinLeave.ViewAbandonedException {
    List<InternalDistributedMember> joinReqs = new ArrayList<>(10);
    Map<InternalDistributedMember, Integer> joinPorts = new HashMap<>(10);
    Set<InternalDistributedMember> leaveReqs = new HashSet<>(10);
    List<InternalDistributedMember> removalReqs = new ArrayList<>(10);
    List<String> removalReasons = new ArrayList<String>(10);

    NetView oldView = gmsJoinLeave.getView();
    List<InternalDistributedMember> oldMembers;
    if (oldView != null) {
      oldMembers = new ArrayList<>(oldView.getMembers());
    } else {
      oldMembers = Collections.emptyList();
    }
    Set<InternalDistributedMember> oldIDs = new HashSet<>();

    for (DistributionMessage msg : requests) {
      logger.debug("processing request {}", msg);

      InternalDistributedMember mbr;
      switch (msg.getDSFID()) {
        case JOIN_REQUEST:
          JoinRequestMessage jmsg = (JoinRequestMessage) msg;
          mbr = jmsg.getMemberID();
          int port = jmsg.getFailureDetectionPort();
          if (!joinReqs.contains(mbr)) {
            if (mbr.getVmViewId() >= 0 && oldMembers.contains(mbr)) {
              // already joined in a previous view
              logger.info("Ignoring join request from member {} who has already joined", mbr);
            } else {
              joinReqs.add(mbr);
              joinPorts.put(mbr, port);
            }
          }
          break;
        case LEAVE_REQUEST_MESSAGE:
          mbr = ((LeaveRequestMessage) msg).getMemberID();
          if (oldMembers.contains(mbr) && !leaveReqs.contains(mbr)) {
            leaveReqs.add(mbr);
          }
          break;
        case REMOVE_MEMBER_REQUEST:
          // process these after gathering all leave-requests so that
          // we don't kick out a member that's shutting down
          break;
        default:
          logger.warn("Unknown membership request encountered: {}", msg);
          break;
      }
    }

    for (DistributionMessage msg : requests) {
      switch (msg.getDSFID()) {
        case REMOVE_MEMBER_REQUEST:
          InternalDistributedMember mbr = ((RemoveMemberMessage) msg).getMemberID();
          if (!leaveReqs.contains(mbr)) {
            if (oldMembers.contains(mbr) && !removalReqs.contains(mbr)) {
              removalReqs.add(mbr);
              removalReasons.add(((RemoveMemberMessage) msg).getReason());
            } else {
              // unknown, probably rogue, process - send it a removal message
              gmsJoinLeave.sendRemoveMessages(Collections.singletonList(mbr),
                  Collections.singletonList(((RemoveMemberMessage) msg).getReason()),
                  new HashSet<>());
            }
          }
          break;
        default:
          break;
      }
    }

    for (InternalDistributedMember mbr : oldIDs) {
      if (!leaveReqs.contains(mbr) && !removalReqs.contains(mbr)) {
        removalReqs.add(mbr);
        removalReasons.add("Removal of old ID that has been reused");
      }
    }

    if (removalReqs.isEmpty() && leaveReqs.isEmpty() && joinReqs.isEmpty()) {
      return;
    }

    NetView newView;
    synchronized (gmsJoinLeave.getViewInstallationLock()) {
      int viewNumber = 0;
      List<InternalDistributedMember> mbrs;
      if (gmsJoinLeave.getView() == null) {
        mbrs = new ArrayList<InternalDistributedMember>();
      } else {
        viewNumber = gmsJoinLeave.getView().getViewId() + 1;
        mbrs = new ArrayList<InternalDistributedMember>(oldMembers);
      }
      mbrs.removeAll(leaveReqs);
      mbrs.removeAll(removalReqs);
      // add joinReqs after removing old members because an ID may
      // be reused in an auto-reconnect and get a new vmViewID
      mbrs.addAll(joinReqs);
      newView = new NetView(localAddress, viewNumber, mbrs, leaveReqs,
          new HashSet<InternalDistributedMember>(removalReqs));
      for (InternalDistributedMember mbr : joinReqs) {
        if (mbrs.contains(mbr)) {
          newView.setFailureDetectionPort(mbr, joinPorts.get(mbr));
        }
      }
      final NetView currentView = gmsJoinLeave.getView();
      if (currentView != null) {
        newView.setFailureDetectionPorts(currentView);
        newView.setPublicKeys(currentView);
      }
    }

    // if there are no membership changes then abort creation of
    // the new view
    if (joinReqs.isEmpty() && newView.getMembers().equals(gmsJoinLeave.getView().getMembers())) {
      logger.info("membership hasn't changed - aborting new view {}", newView);
      return;
    }

    for (InternalDistributedMember mbr : joinReqs) {
      if (mbr.getVmViewId() < 0) {
        mbr.setVmViewId(newView.getViewId());
      }
    }

    if (isShutdown()) {
      return;
    }

    // send removal messages before installing the view so we stop
    // getting messages from members that have been kicked out
    gmsJoinLeave.sendRemoveMessages(removalReqs, removalReasons, oldIDs);

    prepareAndSendView(newView, joinReqs, leaveReqs, newView.getCrashedMembers());

    return;
  }

  /**
   * This handles the 2-phase installation of the view
   *
   */
  void prepareAndSendView(NetView newView, List<InternalDistributedMember> joinReqs,
      Set<InternalDistributedMember> leaveReqs, Set<InternalDistributedMember> removalReqs)
      throws InterruptedException, GMSJoinLeave.ViewAbandonedException {
    boolean prepared;
    do {
      if (this.shutdown || Thread.currentThread().isInterrupted()) {
        return;
      }

      if (gmsJoinLeave.isQuorumRequired() && gmsJoinLeave.isNetworkPartition(newView, true)) {
        gmsJoinLeave.sendNetworkPartitionMessage(newView);
        Thread.sleep(BROADCAST_MESSAGE_SLEEP_TIME);

        Set<InternalDistributedMember> crashes = newView.getActualCrashedMembers(gmsJoinLeave.getView());
        gmsJoinLeave.forceDisconnect(String.format(
            "Exiting due to possible network partition event due to loss of %s cache processes: %s",
            crashes.size(), crashes));
        setShutdownFlag();
        return;
      }

      prepared = gmsJoinLeave.prepareView(newView, joinReqs);
      logger.debug("view preparation phase completed.  prepared={}", prepared);

      NetView conflictingView = gmsJoinLeave.prepareProcessor.getConflictingView();
      if (conflictingView == null) {
        conflictingView = gmsJoinLeave.getPreparedView();
      }

      if (prepared) {
        break;
      }

      Set<InternalDistributedMember> unresponsive = gmsJoinLeave.prepareProcessor.getUnresponsiveMembers();
      unresponsive.removeAll(removalReqs);
      unresponsive.removeAll(leaveReqs);
      if (!unresponsive.isEmpty()) {
        removeHealthyMembers(unresponsive);
        synchronized (viewRequests) {
          // now lets get copy of it in viewRequests sync, as other thread might be accessing it
          unresponsive = new HashSet<>(unresponsive);
        }
      }

      logger.debug("unresponsive members that could not be reached: {}", unresponsive);
      List<InternalDistributedMember> failures =
          new ArrayList<>(gmsJoinLeave.getView().getCrashedMembers().size() + unresponsive.size());

      boolean conflictingViewNotFromMe =
          conflictingView != null && !conflictingView.getCreator().equals(localAddress)
              && conflictingView.getViewId() > newView.getViewId();
      if (conflictingViewNotFromMe) {
        boolean conflictingViewIsMostRecent = (lastConflictingView == null
            || conflictingView.getViewId() > lastConflictingView.getViewId());
        if (conflictingViewIsMostRecent) {
          lastConflictingView = conflictingView;
          // if I am not a locator and the conflicting view is from a locator I should
          // let it take control and stop sending membership views
          if (localAddress.getVmKind() != ClusterDistributionManager.LOCATOR_DM_TYPE
              && conflictingView.getCreator()
                  .getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE) {
            logger.info("View preparation interrupted - a locator is taking over as "
                + "membership coordinator in this view: {}", conflictingView);
            abandonedViews++;
            throw new GMSJoinLeave.ViewAbandonedException();
          }
          logger.info("adding these crashed members from a conflicting view to the crash-set "
              + "for the next view: {}\nconflicting view: {}", unresponsive, conflictingView);
          failures.addAll(conflictingView.getCrashedMembers());
          // this member may have been kicked out of the conflicting view
          if (failures.contains(localAddress)) {
            gmsJoinLeave.forceDisconnect("I am no longer a member of the distributed system");
            setShutdownFlag();
            return;
          }
          List<InternalDistributedMember> newMembers = conflictingView.getNewMembers();
          if (!newMembers.isEmpty()) {
            logger.info("adding these new members from a conflicting view to the new view: {}",
                newMembers);
            for (InternalDistributedMember mbr : newMembers) {
              int port = conflictingView.getFailureDetectionPort(mbr);
              newView.add(mbr);
              newView.setFailureDetectionPort(mbr, port);
              joinReqs.add(mbr);
            }
          }
          // trump the view ID of the conflicting view so mine will be accepted
          if (conflictingView.getViewId() >= newView.getViewId()) {
            newView = new NetView(newView, conflictingView.getViewId() + 1);
          }
        }
      }

      if (!unresponsive.isEmpty()) {
        logger.info("adding these unresponsive members to the crash-set for the next view: {}",
            unresponsive);
        failures.addAll(unresponsive);
      }

      failures.removeAll(removalReqs);
      failures.removeAll(leaveReqs);
      prepared = failures.isEmpty();
      if (!prepared) {
        // abort the current view and try again
        removalReqs.addAll(failures);
        List<InternalDistributedMember> newMembers = new ArrayList<>(newView.getMembers());
        newMembers.removeAll(removalReqs);
        NetView tempView = new NetView(localAddress, newView.getViewId() + 1, newMembers,
            leaveReqs, removalReqs);
        for (InternalDistributedMember mbr : newView.getMembers()) {
          if (tempView.contains(mbr)) {
            tempView.setFailureDetectionPort(mbr, newView.getFailureDetectionPort(mbr));
          }
        }
        newView = tempView;
        int size = failures.size();
        List<String> reasons = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          reasons.add(
              "Failed to acknowledge a new membership view and then failed tcp/ip connection attempt");
        }
        gmsJoinLeave.sendRemoveMessages(failures, reasons, new HashSet<InternalDistributedMember>());
      }

      // if there is no conflicting view then we can count
      // the current state as being prepared. All members
      // who are going to ack have already done so or passed
      // a liveness test
      if (conflictingView == null) {
        prepared = true;
      }

    } while (!prepared);

    lastConflictingView = null;

    gmsJoinLeave.sendView(newView, joinReqs);

    // we also send a join response so that information like the multicast message digest
    // can be transmitted to the new members w/o including it in the view message

    if (markViewCreatorForShutdown && gmsJoinLeave.getViewCreator() != null) {
      markViewCreatorForShutdown = false;
      setShutdownFlag();
    }

    // after sending a final view we need to stop this thread if
    // the GMS is shutting down
    if (gmsJoinLeave.isStopping()) {
      setShutdownFlag();
    }
  }

  /**
   * performs health checks on the collection of members, removing any that are found to be
   * healthy
   *
   */
  private void removeHealthyMembers(final Set<InternalDistributedMember> suspects)
      throws InterruptedException {
    List<Callable<InternalDistributedMember>> checkers =
        new ArrayList<Callable<InternalDistributedMember>>(suspects.size());

    Set<InternalDistributedMember> newRemovals = new HashSet<>();
    Set<InternalDistributedMember> newLeaves = new HashSet<>();

    filterMembers(suspects, newRemovals, REMOVE_MEMBER_REQUEST);
    filterMembers(suspects, newLeaves, LEAVE_REQUEST_MESSAGE);
    newRemovals.removeAll(newLeaves); // if we received a Leave req the member is "healthy"

    suspects.removeAll(newLeaves);

    for (InternalDistributedMember mbr : suspects) {
      if (newRemovals.contains(mbr) || newLeaves.contains(mbr)) {
        continue; // no need to check this member - it's already been checked or is leaving
      }
      checkers.add(new Callable<InternalDistributedMember>() {
        @Override
        public InternalDistributedMember call() throws Exception {
          boolean available = gmsJoinLeave.checkIfAvailable(mbr);

          synchronized (viewRequests) {
            if (available) {
              suspects.remove(mbr);
            }
            viewRequests.notifyAll();
          }
          return mbr;
        }

        @Override
        public String toString() {
          return mbr.toString();
        }
      });
    }

    if (checkers.isEmpty()) {
      logger.debug("all unresponsive members are already scheduled to be removed");
      return;
    }

    logger.debug("checking availability of these members: {}", checkers);
    ExecutorService svc =
        LoggingExecutors.newFixedThreadPool("Geode View Creator verification thread ",
            true, suspects.size());
    try {
      long giveUpTime = System.currentTimeMillis() + viewAckTimeout;
      // submit the tasks that will remove dead members from the suspects collection
      submitAll(svc, checkers);

      // now wait for the tasks to do their work
      long waitTime = giveUpTime - System.currentTimeMillis();
      synchronized (viewRequests) {
        while (waitTime > 0) {
          logger.debug("removeHealthyMembers: mbrs" + suspects.size());

          filterMembers(suspects, newRemovals, REMOVE_MEMBER_REQUEST);
          filterMembers(suspects, newLeaves, LEAVE_REQUEST_MESSAGE);
          newRemovals.removeAll(newLeaves);

          suspects.removeAll(newLeaves);

          if (suspects.isEmpty() || newRemovals.containsAll(suspects)) {
            break;
          }

          viewRequests.wait(waitTime);
          waitTime = giveUpTime - System.currentTimeMillis();
        }
      }
    } finally {
      svc.shutdownNow();
    }
  }

  /**
   * This gets pending requests and returns the IDs of any that are in the given collection
   *
   * @param mbrs collection of IDs to search for
   * @param matchingMembers collection to store matching IDs in
   * @param requestType leave/remove/join
   */
  protected void filterMembers(Collection<InternalDistributedMember> mbrs,
                               Set<InternalDistributedMember> matchingMembers, short requestType) {
    Set<InternalDistributedMember> requests = gmsJoinLeave.getPendingRequestIDs(requestType);

    if (!requests.isEmpty()) {
      logger.debug(
          "filterMembers: processing " + requests.size() + " requests for type " + requestType);
      Iterator<InternalDistributedMember> itr = requests.iterator();
      while (itr.hasNext()) {
        InternalDistributedMember memberID = itr.next();
        if (mbrs.contains(memberID)) {
          testFlagForRemovalRequest = true;
          matchingMembers.add(memberID);
        }
      }
    }
  }

  private <T> List<Future<T>> submitAll(ExecutorService executor,
                                        Collection<? extends Callable<T>> tasks) {
    List<Future<T>> result = new ArrayList<Future<T>>(tasks.size());

    for (Callable<T> task : tasks) {
      result.add(executor.submit(task));
    }

    return result;
  }

  boolean getTestFlagForRemovalRequest() {
    return testFlagForRemovalRequest;
  }

}
