package org.apache.geode.distributed.internal.membership.gms.membership

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select

typealias BatchFrequencyMillis = Long
val NO_BATCHES: BatchFrequencyMillis = -1       // produce no batches at all

// TODO: Figure out why good ole' java.util.function.Predicate gives compile error when I use it
typealias Predicate<T> = (T) -> Boolean

/**
 * This is a coroutine builder. The coroutine it builds is a producer (channel/coroutine) that
 * groups elements from the input into time windows of [periodMillis]
 *
 * If you want a snapshot of the currently-buffered elements at a given point in time, you can
 * send a [CompletableDeferred] to [snapshotRequests] and receive a (async) snapshot.
 *
 * @param input is the input [Channel]
 * @param snapshotRequests is for async snapshot requests
 * @param filterRequests takes a predicate to filter the pending messages
 * @param batchFrequencyRequests takes adjustments to batch frequency (including turning batching
 * off entrely via [NO_BATCHES]
 * @return a [Channel] of batches of elements
 */
@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
fun <T> CoroutineScope.timeWindow(
        input: ReceiveChannel<T>,
        snapshotRequests: Channel<CompletableDeferred<Collection<T>>>,
        filterRequests: Channel<Predicate<T>>,
        batchFrequencyRequests: Channel<BatchFrequencyMillis>) =
        produce<Collection<T>>(coroutineContext + CoroutineName("Time Window Coroutine")) {

            var seen = mutableListOf<T>()

            /*
            This is a channel that will never produce anything.
            ticker can't be nullable because of this compiler bug:

                     https://github.com/Kotlin/kotlinx.coroutines/issues/448

            ticker?.onReceive {...} in the select block causes a runtime error.

            Though a Channel is pretty lightweight (it has a queue object and little else)
            it might be be nice to make an even lighter-weight, immutable one for this purpose.
            */
            val neverTick = Channel<Unit>()

            var ticker: ReceiveChannel<Unit> = neverTick

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

                    filterRequests.onReceive {
                        seen = seen.filter(it).toMutableList()
                    }

                    batchFrequencyRequests.onReceive {
                        if (ticker != neverTick)
                            ticker.cancel()
                        if (it == NO_BATCHES)
                            ticker = neverTick
                        else
                            ticker = ticker(it)
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