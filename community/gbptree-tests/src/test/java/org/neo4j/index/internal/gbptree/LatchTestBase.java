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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.function.Predicates.alwaysTrue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.test.OtherThreadExecutor;

abstract class LatchTestBase {
    OtherThreadExecutor t2;

    @BeforeEach
    void startT2() {
        t2 = new OtherThreadExecutor(TreeNodeLatchService.class.getName());
    }

    @AfterEach
    void stopT2() {
        t2.close();
    }

    Future<Void> beginAndAwaitLatchAcquisition(Runnable lockFunction) throws TimeoutException {
        // Let t2 do a lock acquisition which is expected to block
        Future<Void> readAcquisition = t2.executeDontWait(() -> {
            lockFunction.run();
            return null;
        });

        // Make sure it's blocking
        for (int consecutiveHits = 0; consecutiveHits < 10; consecutiveHits++) {
            if (!t2.waitUntil(alwaysTrue()).isAt(LongSpinLatch.class, "spinTransform")) {
                consecutiveHits = 0;
            }
        }

        // Return the future
        return readAcquisition;
    }

    static class LongCapture implements LongConsumer {
        final AtomicInteger count = new AtomicInteger();
        volatile long captured;

        @Override
        public void accept(long value) {
            this.captured = value;
            this.count.incrementAndGet();
        }
    }
}
