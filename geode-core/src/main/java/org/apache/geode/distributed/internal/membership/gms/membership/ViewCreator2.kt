package org.apache.geode.distributed.internal.membership.gms.membership

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import org.apache.geode.distributed.internal.DistributionMessage
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

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

/**
 * This class provides convenient communication with the creator coroutine(s) from Java.
 * It's a [CoroutineScope] that can be [destroy]ed to stop all the coroutines it has
 * (directly or indirectly) started.
 */
class ViewCreator2(
        override val coroutineContext: CoroutineContext = EmptyCoroutineContext) : CoroutineScope {

    // TODO: figure out if I need to create a new context or if it's ok to reuse parent context directly

    val messageChannel: Channel<DistributionMessage> = Channel()
    val snapshotRequestChannel: Channel<CompletableDeferred<Collection<DistributionMessage>>> =
            Channel()

    init {
        log("BOOM! Constructed!")
        val messageBatches = timeWindow(messageChannel, snapshotRequestChannel, 300)
        viewCreator(messageBatches)
    }

    // fire-and-forget
    fun submit(msg: DistributionMessage) =
            launch(coroutineContext + CoroutineName("Membership View Creator submit()")) {
                messageChannel.send(msg)
            }

    /**
     * request-respone
     */
    fun snapshot(): Collection<DistributionMessage> =
            runBlocking(coroutineContext + CoroutineName("Membership View Creator snapshot()")) {
                with(CompletableDeferred<Collection<DistributionMessage>>()) {
                    snapshotRequestChannel.send(this)
                    await()
                }
            }
}
