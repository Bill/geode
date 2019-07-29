package org.apache.geode.distributed.internal.membership.gms.membership

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select

/**
 * This is a coroutine builder. The coroutine it builds is a producer (channel/coroutine) that
 * groups elements from the input into time windows of [periodMillis]
 *
 * If you want a snapshot of the currently-buffered elements at a given point in time, you can
 * send a [CompletableDeferred] to [snapshotRequests] and receive a (async) snapshot.
 *
 * @param input is the input [Channel]
 * @param periodMillis is the time period, in milliseconds
 * @param snapshotRequests is for async snapshot requests
 * @return a [Channel] of batches of elements
 */
@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
fun <T> CoroutineScope.timeWindow(
        input: ReceiveChannel<T>,
        snapshotRequests: Channel<CompletableDeferred<Collection<T>>>,
        periodMillis: Long) =
        produce<Collection<T>>(coroutineContext + CoroutineName("Time Window Coroutine")) {

            val ticker = ticker(periodMillis)

            var seen = mutableListOf<T>()

            while (true) {

                select<Unit> {

                    input.onReceive {
                        log("received input: $it")
                        seen.add(it)
                    }

                    snapshotRequests.onReceive {
                        log("received snapshot request")
                        it.complete(seen.toList())
                    }

                    ticker.onReceive {
                        if (seen.isNotEmpty())
                            log("producing batch: $seen")
                        send(seen)
                        seen = mutableListOf()
                    }
                }
            }
        }