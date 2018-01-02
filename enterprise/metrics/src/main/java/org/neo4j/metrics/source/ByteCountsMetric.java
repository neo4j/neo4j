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

    public void bytesWritten( long bytes )
    {
        bytesWritten.inc( bytes );
    }

    public void bytesRead( long bytes )
    {
        bytesRead.inc( bytes );
    }
}
