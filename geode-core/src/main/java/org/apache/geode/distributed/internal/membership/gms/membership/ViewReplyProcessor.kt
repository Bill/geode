package org.apache.geode.distributed.internal.membership.gms.membership

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.whileSelect
import java.util.HashSet

import org.apache.logging.log4j.Logger

import org.apache.geode.distributed.internal.membership.InternalDistributedMember
import org.apache.geode.distributed.internal.membership.NetView
import org.apache.geode.distributed.internal.membership.gms.Services
import kotlin.coroutines.CoroutineContext

internal sealed class ChangeIsWaitingRoleRequest

internal class StartWaiting(val viewId: Int, val recips: Set<InternalDistributedMember>) : ChangeIsWaitingRoleRequest() {
}

internal object StopWaiting : ChangeIsWaitingRoleRequest() {
}

internal class ViewReplyProcessor(val name: String,
                                  val services: Services,
                                  val viewAckTimeout: Int,
                                  val logger: Logger,
                                  coroutineContext: CoroutineContext) {

    private val coroutineScope = CoroutineScope(coroutineContext)

    private val changeIsWaitingRequests: Channel<ChangeIsWaitingRoleRequest> = Channel()


    init {
        notWaiting()
    }

    internal fun notWaiting(
            notRepliedYet: MutableSet<InternalDistributedMember> = HashSet(),
            pendingRemovals: MutableSet<InternalDistributedMember> = HashSet()
    ): Job =
            coroutineScope.launch(CoroutineName("View Reply Processor: $name")) {


                whileSelect {

                    changeIsWaitingRequests.onReceive {
                        when (it) {
                            is StartWaiting -> {
                                waiting(it.viewId, it.recips, notRepliedYet, pendingRemovals)
                                false
                            }
                            is StopWaiting -> true
                        }
                    }
                }


            }

    internal fun waiting(
            viewId: Int,
            recips: Set<InternalDistributedMember>,
            notRepliedYet: MutableSet<InternalDistributedMember>,
            pendingRemovals: MutableSet<InternalDistributedMember>): Job =
            coroutineScope.launch(CoroutineName("View Reply Processor: $name")) {

                var conflictingView: NetView? = null
                var conflictingViewSender: InternalDistributedMember? = null

                notRepliedYet.clear()
                notRepliedYet.addAll(recips)
                pendingRemovals.clear()

                whileSelect {


                    //stuff

                    changeIsWaitingRequests.onReceive {
                        when (it) {
                            is StartWaiting -> true
                            is StopWaiting -> {
                                notWaiting(notRepliedYet, pendingRemovals)
                                false
                            }
                        }
                    }

                }


            }

    @Volatile
    var viewId = -1
    val notRepliedYet: MutableSet<InternalDistributedMember> = HashSet()
    var conflictingView: NetView? = null
    var conflictingViewSender: InternalDistributedMember? = null
    @Volatile
    var isWaiting: Boolean = false
    val pendingRemovals: MutableSet<InternalDistributedMember> = HashSet()

    val unresponsiveMembers: Set<InternalDistributedMember>
        @Synchronized get() = HashSet(this.notRepliedYet)


    fun initialize(viewId: Int, recips: Set<InternalDistributedMember>) = runBlocking {
        changeIsWaitingRequests.send(StartWaiting(viewId,recips))
    }

    @Synchronized
    fun processPendingRequests(pendingLeaves: Set<InternalDistributedMember>,
                               pendingRemovals: Set<InternalDistributedMember>) {
        // there's no point in waiting for members who have already
        // requested to leave or who have been declared crashed.
        // We don't want to mix the two because pending removals
        // aren't reflected as having crashed in the current view
        // and need to cause a new view to be generated
        for (mbr in pendingLeaves) {
            notRepliedYet.remove(mbr)
        }
        for (mbr in pendingRemovals) {
            if (this.notRepliedYet.contains(mbr)) {
                this.pendingRemovals.add(mbr)
            }
        }
    }

    @Synchronized
    fun memberSuspected(suspect: InternalDistributedMember) {
        if (isWaiting) {
            // we will do a final check on this member if it hasn't already
            // been done, so stop waiting for it now
            logger.debug("view response processor recording suspect status for {}", suspect)
            if (notRepliedYet.contains(suspect) && !pendingRemovals.contains(suspect)) {
                pendingRemovals.add(suspect)
                checkIfDone()
            }
        }
    }

    @Synchronized
    fun processLeaveRequest(mbr: InternalDistributedMember) {
        if (isWaiting) {
            logger.debug("view response processor recording leave request for {}", mbr)
            stopWaitingFor(mbr)
        }
    }

    @Synchronized
    fun processRemoveRequest(mbr: InternalDistributedMember) {
        if (isWaiting) {
            logger.debug("view response processor recording remove request for {}", mbr)
            pendingRemovals.add(mbr)
            checkIfDone()
        }
    }

    @Synchronized
    fun processViewResponse(viewId: Int, sender: InternalDistributedMember,
                            conflictingView: NetView?) {
        if (!isWaiting) {
            return
        }

        if (viewId == this.viewId) {
            if (conflictingView != null) {
                this.conflictingViewSender = sender
                this.conflictingView = conflictingView
            }

            logger.debug("view response processor recording response for {}", sender)
            stopWaitingFor(sender)
        }
    }

    /**
     * call with synchronized(this)
     */
    private fun stopWaitingFor(mbr: InternalDistributedMember) {
        notRepliedYet.remove(mbr)
        checkIfDone()
    }

    /**
     * call with synchronized(this)
     */
    private fun checkIfDone() {
        if (notRepliedYet.isEmpty() || pendingRemovals != null && pendingRemovals.containsAll(notRepliedYet)) {
            logger.debug("All anticipated view responses received - notifying waiting thread")
            isWaiting = false
            (this as Object).notifyAll()
        } else {
            logger.debug("Still waiting for these view replies: {}", notRepliedYet)
        }
    }

    @Throws(InterruptedException::class)
    fun waitForResponses(): Set<InternalDistributedMember> {
        val result: MutableSet<InternalDistributedMember>
        val endOfWait = System.currentTimeMillis() + viewAckTimeout
        try {
            while (System.currentTimeMillis() < endOfWait && !services.cancelCriterion.isCancelInProgress) {
                try {
                    if (!synchronized(this) {
                        if (!isWaiting || this.notRepliedYet.isEmpty() || this.conflictingView != null) {
                           false
                        }
                        else {
                            (this as Object).wait(1000)
                            true
                        }
                    })
                        break
                } catch (e: InterruptedException) {
                    logger.debug("Interrupted while waiting for view responses")
                    throw e
                }

            }
        } finally {
            synchronized(this) {
                if (!this.isWaiting) {
                    // if we've set waiting to false due to incoming messages then
                    // we've discounted receiving any other responses from the
                    // remaining members due to leave/crash notification
                    result = HashSet(pendingRemovals)
                } else {
                    result = HashSet(this.notRepliedYet)
                    result.addAll(pendingRemovals)
                    this.isWaiting = false
                }
            }
        }
        return result
    }
}
