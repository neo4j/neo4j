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
