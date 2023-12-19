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

import org.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogAppendIndexMonitor;

public class RaftLogAppendIndexMetric implements RaftLogAppendIndexMonitor
{
    private AtomicLong appendIndex = new AtomicLong( 0 );

    @Override
    public long appendIndex()
    {
        return appendIndex.get();
    }

    @Override
    public void appendIndex( long appendIndex )
    {
        this.appendIndex.set( appendIndex );
    }
}
