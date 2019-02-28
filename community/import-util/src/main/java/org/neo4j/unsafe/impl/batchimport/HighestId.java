/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks a highest id when there are potentially multiple concurrent threads calling {@link #offer(long)}.
 */
public class HighestId
{
    private final AtomicLong highestId;

    public HighestId()
    {
        this( 0 );
    }

    public HighestId( long initialId )
    {
        this.highestId = new AtomicLong( initialId );
    }

    public void offer( long candidate )
    {
        long currentHighest;
        do
        {
            currentHighest = highestId.get();
            if ( candidate <= currentHighest )
            {
                return;
            }
        }
        while ( !highestId.compareAndSet( currentHighest, candidate ) );
    }

    public long get()
    {
        return highestId.get();
    }
}
