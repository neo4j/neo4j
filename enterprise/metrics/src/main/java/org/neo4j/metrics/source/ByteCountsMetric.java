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
package org.neo4j.metrics.source;

import com.codahale.metrics.Counter;

import org.neo4j.kernel.monitoring.ByteCounterMonitor;

public class ByteCountsMetric implements ByteCounterMonitor
{
    private final Counter bytesWritten = new Counter();
    private final Counter bytesRead = new Counter();

    public long getBytesWritten()
    {
        return bytesWritten.getCount();
    }

    public long getBytesRead()
    {
        return bytesRead.getCount();
    }

    @Override
    public void bytesWritten( long bytes )
    {
        bytesWritten.inc( bytes );
    }

    @Override
    public void bytesRead( long bytes )
    {
        bytesRead.inc( bytes );
    }
}
