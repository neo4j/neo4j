/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.cache;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

/**
 * This cache is not meant for production use, but it can be useful
 * in various investigative circumstances.
 */
public class UnboundedInFlightCache implements InFlightCache
{
    private Map<Long,RaftLogEntry> map = new HashMap<>();
    private boolean enabled;

    @Override
    public synchronized void enable()
    {
        enabled = true;
    }

    @Override
    public synchronized void put( long logIndex, RaftLogEntry entry )
    {
        if ( !enabled )
        {
            return;
        }

        map.put( logIndex, entry );
    }

    @Override
    public synchronized RaftLogEntry get( long logIndex )
    {
        if ( !enabled )
        {
            return null;
        }

        return map.get( logIndex );
    }

    @Override
    public synchronized void truncate( long fromIndex )
    {
        if ( !enabled )
        {
            return;
        }

        map.keySet().removeIf( idx -> idx >= fromIndex );
    }

    @Override
    public synchronized void prune( long upToIndex )
    {
        if ( !enabled )
        {
            return;
        }

        map.keySet().removeIf( idx -> idx <= upToIndex );
    }

    @Override
    public synchronized long totalBytes()
    {
        // not updated correctly
        return 0;
    }

    @Override
    public synchronized int elementCount()
    {
        // not updated correctly
        return 0;
    }
}
