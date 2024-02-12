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

final class TimeConstrainedCounter {
    private final long windowMillis;
    private final Clock clock;
    private final AtomicReference<State> state = new AtomicReference<>(new State(0, 0));

    TimeConstrainedCounter(long windowMillis, Clock clock) {
        this.windowMillis = windowMillis;
        this.clock = clock;
    }

    public void increment() {
        this.incrementAndGet(1);
    }

    public long incrementAndGet() {
        return this.incrementAndGet(1);
    }

    public long incrementAndGet(long delta) {
        State previous;
        State next;
        do {
            previous = this.state.get();
            var now = this.clock.millis();

            if (now - previous.lastUpdate >= this.windowMillis) {
                next = new State(delta, now);
            } else {
                next = new State(previous.counter() + delta, now);
            }
        } while (!this.state.compareAndSet(previous, next));

        return next.counter;
    }

    private record State(long counter, long lastUpdate) {}
}
