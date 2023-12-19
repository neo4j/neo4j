/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.metrics.source.causalclustering;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.causalclustering.catchup.tx.PullRequestMonitor;

class PullRequestMetric implements PullRequestMonitor
{
    private AtomicLong lastRequestedTxId = new AtomicLong( 0 );
    private AtomicLong lastReceivedTxId = new AtomicLong( 0 );
    private LongAdder events = new LongAdder(  );

    @Override
    public void txPullRequest( long txId )
    {
        events.increment();
        this.lastRequestedTxId.set( txId );
    }

    @Override
    public void txPullResponse( long txId )
    {
        lastReceivedTxId.set( txId );
    }

    @Override
    public long lastRequestedTxId()
    {
        return this.lastRequestedTxId.get();
    }

    @Override
    public long numberOfRequests()
    {
        return events.longValue();
    }

    @Override
    public long lastReceivedTxId()
    {
        return lastReceivedTxId.get();
    }
}
