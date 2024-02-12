/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.connector.accounting.error;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicReference;

final class CircuitBreaker {
    private final long threshold;
    private final long resetMillis;
    private final Listener listener;
    private final Clock clock;

    private final TimeConstrainedCounter counter;

    private final AtomicReference<Long> tripped = new AtomicReference<>(null);

    CircuitBreaker(long threshold, long windowMillis, long resetMillis, Listener listener, Clock clock) {
        this.threshold = threshold;
        this.resetMillis = resetMillis;
        this.listener = listener;
        this.clock = clock;

        this.counter = new TimeConstrainedCounter(windowMillis, clock);
    }

    public void increment() {
        var now = this.clock.millis();
        var newValue = this.counter.incrementAndGet();

        if (newValue > this.threshold) {
            if (this.tripped.compareAndSet(null, now)) {
                this.listener.onTripped();
            } else {
                this.listener.onContinue();
            }
        } else {
            var trippedAt = this.tripped.get();
            if (trippedAt != null
                    && now - trippedAt >= this.resetMillis
                    && this.tripped.compareAndSet(trippedAt, null)) {
                this.listener.onReset();
            } else {
                this.listener.onContinue();
            }
        }
    }

    interface Listener {

        void onTripped();

        void onContinue();

        void onReset();
    }
}
