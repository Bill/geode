package org.apache.geode.distributed.internal.membership.gms.membership

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import org.apache.geode.distributed.internal.DistributionMessage
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/**
 * This is a Kotlin coroutine builder that launches a new view creator coroutine.
 */
fun CoroutineScope.viewCreator(
        distributionMessages: ReceiveChannel<DistributionMessage>,
        snapshotRequests: Channel<CompletableDeferred<Collection<DistributionMessage>>>) = launch {

    var messages = mutableListOf<DistributionMessage>()

    while (true) {
        delay(300)

        select<Unit> {

            distributionMessages.onReceive {
                println("viewCreator received message: $it")
                messages.add(it)
            }

            snapshotRequests.onReceive {
                println("viewCreator received snapshot request")
                it.complete(messages.toList())
            }
        }
    }

}

/**
 * This is a Java-callable class to wrap the Kotlin coroutine builder (CoroutineScope.viewCreator())
 */
class ViewCreator2(
        override val coroutineContext: CoroutineContext = EmptyCoroutineContext) : CoroutineScope {

    // TODO: figure out if I need to create a new context or if it's ok to reuse parent context directly

    val submissionChannel: Channel<DistributionMessage> = Channel()
    val snapshotRequestChannel: Channel<CompletableDeferred<Collection<DistributionMessage>>> =
            Channel()

    init {
        viewCreator(submissionChannel, snapshotRequestChannel)
    }

    // fire-and-forget
    fun submit(msg: DistributionMessage) = launch {
        submissionChannel.send(msg)
    }

    /**
     * request-respone
     */
    fun snapshot(): Collection<DistributionMessage> = runBlocking {
        with(CompletableDeferred<Collection<DistributionMessage>>()) {
            snapshotRequestChannel.send(this)
            await()
        }
    }

    fun terminateCoroutines() {
        coroutineContext.cancel()
        println("ViewCreator2 terminated coroutines")
    }
}
