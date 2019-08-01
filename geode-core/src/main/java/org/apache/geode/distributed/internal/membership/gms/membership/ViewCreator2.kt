package org.apache.geode.distributed.internal.membership.gms.membership

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import org.apache.geode.distributed.internal.DistributionMessage
import org.apache.geode.distributed.internal.membership.NetView
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

val VIEW_CREATOR_BATCH_FREQUENCY: Long = 300

/*
 Launch with -Dkotlinx.coroutines.debug JVM option to get coroutine context in thread name
 */
fun log(msg: String) = println("[${Thread.currentThread().name}] $msg")

/**
 * This is a coroutine builder for the view creator (coroutine)
 */
fun CoroutineScope.viewCreator(
        messages: ReceiveChannel<Collection<DistributionMessage>>) =
        launch(coroutineContext + CoroutineName("View Creator Coroutine")) {

            while (true) {
                val msg = messages.receive()
                if (msg.isNotEmpty())
                log("received batch: $msg")
            }
}

private sealed class State {
    abstract suspend fun setIsCoordinator(
            becomeCoordinator: Boolean,
            batchFrequencyRequests: SendChannel<BatchFrequencyMillis>): State
    abstract suspend fun newViewInstalled(
            newView: NetView,
            filterRequests: Channel<Predicate<DistributionMessage>>)
}

private object CoordinatorState : State() {
    override suspend fun setIsCoordinator(
            becomeCoordinator: Boolean,
            batchFrequencyRequests: SendChannel<BatchFrequencyMillis>): State {
        if (becomeCoordinator) {
            return this
        } else {
            // Stop producing batches
            batchFrequencyRequests.send(NO_BATCHES)
            return NotCoordinatorState
        }
    }

    override suspend fun newViewInstalled(
            newView: NetView,
            filterRequests: Channel<Predicate<DistributionMessage>>) {
        // no-op
    }
}

private object NotCoordinatorState : State() {
    override suspend fun setIsCoordinator(
            becomeCoordinator: Boolean,
            batchFrequencyRequests: SendChannel<BatchFrequencyMillis>): State {
        if (becomeCoordinator) {
            batchFrequencyRequests.send(VIEW_CREATOR_BATCH_FREQUENCY)
            return CoordinatorState
        } else {
            return this
        }
    }

    override suspend fun newViewInstalled(
            newView: NetView,
            filterRequests: Channel<Predicate<DistributionMessage>>) {

        // describe messages to keep: keep those messages whose members are not mentioned in view
        fun filter(msg: DistributionMessage): Boolean =
            !when(msg) {
                is JoinRequestMessage -> newView.contains(msg.memberID)
                is LeaveRequestMessage -> newView.contains(msg.memberID)
                is RemoveMemberMessage -> newView.contains(msg.memberID)
                else -> false // keep all other kinds of message
            }

        /*
         When we are not the coordinator, and a new view is installed, use the members
         in that view to filter out messages that have accumulated. Messages are accumulating
         because we turned off batch production when we moved into this state (NotCoordinatorState)
         */
        filterRequests.send(::filter)
    }
}

/**
 * This class provides convenient communication with the creator coroutine(s) from Java.
 * It's a [CoroutineScope] that can be [destroy]ed to stop all the coroutines it has
 * (directly or indirectly) started.
 */
class ViewCreator2(
        override val coroutineContext: CoroutineContext = EmptyCoroutineContext) : CoroutineScope {

    // TODO: figure out if I need to create a new context or if it's ok to reuse parent context directly

    private val messages: Channel<DistributionMessage> = Channel()
    private val snapshotRequests: Channel<CompletableDeferred<Collection<DistributionMessage>>> =
            Channel()
    private val filterRequests: Channel<Predicate<DistributionMessage>> = Channel()
    private val batchFrequencyRequests: Channel<BatchFrequencyMillis> = Channel()
    private var state: State = NotCoordinatorState

    init {
        log("BOOM! Constructed!")
        val messageBatches = timeWindow(messages, snapshotRequests, filterRequests, batchFrequencyRequests)
        viewCreator(messageBatches)
    }

    // fire-and-forget
    fun submit(msg: DistributionMessage) =
            launch(coroutineContext + CoroutineName("Membership View Creator submit()")) {
                messages.send(msg)
            }

    /**
     * request-respone
     */
    fun snapshot(): Collection<DistributionMessage> =
            runBlocking(coroutineContext + CoroutineName("Membership View Creator snapshot()")) {
                with(CompletableDeferred<Collection<DistributionMessage>>()) {
                    snapshotRequests.send(this)
                    await()
                }
            }

    /**
     * Call this method to notify view creator that this node has become the coordinator
     * (or that it has relinquished that role)
     * @param becomeCoordinator set to true to inform the view creator that this node has assumed
     * the coordinator role; set to false to inform the view creator that this node has
     * relinquished that role
     */
    fun setIsCoordinator(becomeCoordinator: Boolean) = runBlocking {
        state = state.setIsCoordinator(becomeCoordinator, batchFrequencyRequests)
    }
    
    fun newViewInstalled(newView: NetView) = runBlocking {
        state.newViewInstalled(newView, filterRequests)
    }
}
