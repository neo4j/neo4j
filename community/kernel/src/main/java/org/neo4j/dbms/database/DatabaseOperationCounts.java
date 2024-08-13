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
package org.neo4j.dbms.database;

import java.util.concurrent.atomic.LongAdder;

/**
 * This interface and it's embedded implementation is there to track database operation in Enterprise Editions via Global Metrics.
 */
public interface DatabaseOperationCounts {
    long startCount();

    long createCount();

    long stopCount();

    long dropCount();

    long failedCount();

    long recoveredCount();

    long panicCount();

    class Counter implements DatabaseOperationCounts {
        private final LongAdder createCount = new LongAdder();
        private final LongAdder startCount = new LongAdder();
        private final LongAdder stopCount = new LongAdder();
        private final LongAdder dropCount = new LongAdder();
        private final LongAdder failedCount = new LongAdder();
        private final LongAdder recoveredCount = new LongAdder();
        private final LongAdder panicCount = new LongAdder();

        @Override
        public long startCount() {
            return startCount.sum();
        }

        @Override
        public long createCount() {
            return createCount.sum();
        }

        @Override
        public long stopCount() {
            return stopCount.sum();
        }

        @Override
        public long dropCount() {
            return dropCount.sum();
        }

        @Override
        public long failedCount() {
            return failedCount.sum();
        }

        @Override
        public long recoveredCount() {
            return recoveredCount.sum();
        }

        @Override
        public long panicCount() {
            return panicCount.sum();
        }

        public void increaseCreateCount() {
            createCount.increment();
        }

        public void increaseStartCount() {
            startCount.increment();
        }

        public void increaseStopCount() {
            stopCount.increment();
        }

        public void increaseDropCount() {
            dropCount.increment();
        }

        public void increaseFailedCount() {
            failedCount.increment();
        }

        public void increaseRecoveredCount() {
            recoveredCount.increment();
        }

        public void increasePanicCount() {
            panicCount.increment();
        }
    }
}
