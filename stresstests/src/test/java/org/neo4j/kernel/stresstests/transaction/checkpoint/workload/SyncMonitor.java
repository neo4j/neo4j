/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.workload;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class SyncMonitor implements Worker.Monitor
{
    private final AtomicBoolean stopSignal = new AtomicBoolean();
    private final AtomicLong transactionCounter = new AtomicLong();
    private final CountDownLatch stopLatch;

    public SyncMonitor( int threads )
    {
        this.stopLatch = new CountDownLatch( threads );
    }

    @Override
    public void transactionCompleted()
    {
        transactionCounter.incrementAndGet();
    }

    @Override
    public boolean stop()
    {
        return stopSignal.get();
    }

    @Override
    public void done()
    {
        stopLatch.countDown();
    }

    public long transactions()
    {
        return transactionCounter.get();
    }

    public void stopAndWaitWorkers() throws InterruptedException
    {
        stopSignal.set( true );
        stopLatch.await();
    }
}
