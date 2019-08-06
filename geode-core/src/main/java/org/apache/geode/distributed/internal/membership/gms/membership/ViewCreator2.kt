package org.apache.geode.distributed.internal.membership.gms.membership

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.selects.whileSelect
import org.apache.geode.distributed.DistributedSystemDisconnectedException
import org.apache.geode.distributed.internal.DistributionMessage
import org.apache.geode.distributed.internal.membership.NetView
import org.apache.geode.distributed.internal.membership.gms.interfaces.Locator
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage
import org.apache.geode.distributed.internal.membership.gms.messages.LeaveRequestMessage
import org.apache.geode.distributed.internal.membership.gms.messages.RemoveMemberMessage
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/*
 Launch with -Dkotlinx.coroutines.debug JVM option to get coroutine context in thread name
 */
internal fun log(msg: String) = println("[${Thread.currentThread().name}] $msg")

internal sealed class ChangeCoordinatorRoleRequest

internal class StartCoordinating(val legacyViewCreator: ViewCreator,
                                 val locator: Locator) : ChangeCoordinatorRoleRequest() {
}

internal object StopCoordinating : ChangeCoordinatorRoleRequest() {
}

// TODO: Figure out why good ole' java.util.function.Predicate gives compile error when I use it
typealias Predicate<T> = (T) -> Boolean

/**
 * This class provides convenient communication with the creator coroutine(s) from Java.
 * It's a [CoroutineScope] that can be [cancel]ed to stop all the coroutines it has
 * (directly or indirectly) started.
 */
internal class ViewCreator2(
        override val coroutineContext: CoroutineContext = EmptyCoroutineContext,
        val batchFrequency: Long) : CoroutineScope {

    // TODO: figure out if I need to create a new context or if it's ok to reuse parent context directly

    private val messages: Channel<DistributionMessage> = Channel()
    private val snapshotRequests: Channel<CompletableDeferred<Collection<DistributionMessage>>> =
            Channel()
    private val filterRequests: Channel<Predicate<DistributionMessage>> = Channel()
    private val changeCoordinatorRoleRequests: Channel<ChangeCoordinatorRoleRequest> = Channel()
    private val newViewInstalledRequests: Channel<NetView> = Channel()

    init {
        notCoordinatorState()
    }

    @ObsoleteCoroutinesApi
    @ExperimentalCoroutinesApi
    internal fun CoroutineScope.notCoordinatorState(): Job =
            launch(coroutineContext + CoroutineName("ViewCreator Not Coordinator State Coroutine")) {

                var seen = mutableListOf<DistributionMessage>()

                whileSelect {

                        messages.onReceive {
                            log("received input: $it")
                            seen.add(it)
                            true
                        }

                        snapshotRequests.onReceive {
                            log("received snapshot request")
                            it.complete(seen.toList())
                            true
                        }

                        newViewInstalledRequests.onReceive {

                            // describe messages to keep: keep those messages whose members are not mentioned in view
                            val filter = fun(msg: DistributionMessage):Boolean =
                                !when (msg) {
                                    is JoinRequestMessage -> it.contains(msg.memberID)
                                    is LeaveRequestMessage -> it.contains(msg.memberID)
                                    is RemoveMemberMessage -> it.contains(msg.memberID)
                                    else -> false // keep all other kinds of message
                                }

                            /*
                             When we are not the coordinator, and a new view is installed, use the members
                             in that view to filter out messages that have accumulated. Messages are accumulating
                             because we turned off batch production when we moved into this state (NotCoordinatorState)
                             */
                            seen = seen.filter(filter).toMutableList()
                            true
                        }

                        changeCoordinatorRoleRequests.onReceive {
                            when (it) {
                                is StartCoordinating -> {
                                    cancel()
                                    coordinatorState(it)
                                    false
                                }
                                is StopCoordinating -> true
                            }
                        }
                    }
                }

    @ObsoleteCoroutinesApi
    @ExperimentalCoroutinesApi
    internal fun CoroutineScope.coordinatorState(transitionData: StartCoordinating): Job =
            launch(coroutineContext + CoroutineName("ViewCreator Coordinator State Coroutine")) {

                val legacyViewCreator = transitionData.legacyViewCreator
                val locator = transitionData.locator

                val ticker: ReceiveChannel<Unit> = kotlinx.coroutines.channels.ticker(batchFrequency)

                var seen = mutableListOf<DistributionMessage>()

                log("starting")

                // TODO: send initial view then go to filtering-unresponsive-members state
                legacyViewCreator.sendInitialView()

                whileSelect {

                        messages.onReceive {
                            log("received input: $it")
                            seen.add(it)
                            true
                        }

                        snapshotRequests.onReceive {
                            log("received snapshot request")
                            it.complete(seen.toList())
                            true
                        }

                        filterRequests.onReceive {
                            seen = seen.filter(it).toMutableList()
                            true
                        }

                        ticker.onReceive {
                            if (seen.isNotEmpty()) {
                                log("processing ${seen.size} requests for the next membership view ($seen)")
                                try {
                                    // TODO: send view then go to filtering-unresponsive-members state
                                    legacyViewCreator.createAndSendView(seen)
                                    seen = mutableListOf()
                                } catch (e: Throwable) {
                                    when (e) {
                                        is GMSJoinLeave.ViewAbandonedException -> {
                                            // keep seen messages and we'll try again later
                                        }
                                        else -> {
                                            log("exiting")
                                            cancel()
                                            notCoordinatorState()
                                            locator.setIsCoordinator(false)
                                            legacyViewCreator.informToPendingJoinRequests(seen)
                                            throw e
                                        }
                                    }
                                }
                            }
                            true
                        }

                        changeCoordinatorRoleRequests.onReceive {
                            when (it) {
                                is StartCoordinating -> true
                                is StopCoordinating -> {
                                    cancel()
                                    notCoordinatorState()
                                    false
                                }
                            }
                        }
                    }
                }

    fun submit(msg: DistributionMessage) = runBlocking {
        messages.send(msg)
    }

    /**
     * request-respone
     */
    fun snapshot(): Collection<DistributionMessage> = runBlocking {
        with(CompletableDeferred<Collection<DistributionMessage>>()) {
            snapshotRequests.send(this)
            await()
        }
    }

    /**
     * Call this method to notify view creator that this node has become the coordinator
     */
    fun startCoordinating(legacyViewCreator: ViewCreator,
                          locator: Locator) = runBlocking {
        changeCoordinatorRoleRequests.send(StartCoordinating(legacyViewCreator,locator))
    }

    fun stopCoordinating() = runBlocking {
        changeCoordinatorRoleRequests.send(StopCoordinating)
    }

    fun newViewInstalled(newView: NetView) = runBlocking {
        newViewInstalledRequests.send(newView)
    }
}
