/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.database;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This interface and it's embedded implementation is there to track database operation in Enterprise Editions via Global Metrics.
 */
public interface DatabaseOperationCounts
{
    long startCount();

    long createCount();

    long stopCount();

    long dropCount();

    long failedCount();

    long recoveredCount();

    class Counter implements  DatabaseOperationCounts
    {
        private AtomicLong createCount = new AtomicLong( 0 );
        private AtomicLong startCount = new AtomicLong( 0 );
        private AtomicLong stopCount = new AtomicLong( 0 );
        private AtomicLong dropCount = new AtomicLong( 0 );
        private AtomicLong failedCount = new AtomicLong( 0 );
        private AtomicLong recoveredCount = new AtomicLong( 0 );

        public long startCount()
        {
            return startCount.get();
        }

        public long createCount()
        {
            return createCount.get();
        }

        public long stopCount()
        {
            return stopCount.get();
        }

        public long dropCount()
        {
            return dropCount.get();
        }

        public long failedCount()
        {
            return failedCount.get();
        }

        public long recoveredCount()
        {
            return recoveredCount.get();
        }

        public void increaseCreateCount()
        {
            createCount.incrementAndGet();
        }

        public void increaseStartCount()
        {
            startCount.incrementAndGet();
        }

        public void increaseStopCount()
        {
            stopCount.incrementAndGet();
        }

        public void increaseDropCount()
        {
            dropCount.incrementAndGet();
        }

        public void increaseFailedCount()
        {
            failedCount.incrementAndGet();
        }

        public void increaseRecoveredCount()
        {
            recoveredCount.incrementAndGet();
        }
    }
}
