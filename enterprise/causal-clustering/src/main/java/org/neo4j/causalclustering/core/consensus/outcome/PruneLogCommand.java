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
package org.neo4j.causalclustering.core.consensus.outcome;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.logging.Log;

public class PruneLogCommand implements RaftLogCommand
{
    private final long pruneIndex;

    public PruneLogCommand( long pruneIndex )
    {
        this.pruneIndex = pruneIndex;
    }

    @Override
    public void dispatch( Handler handler ) throws IOException
    {
        handler.prune( pruneIndex );
    }

    @Override
    public void applyTo( RaftLog raftLog, Log log ) throws IOException
    {
        raftLog.prune( pruneIndex );
    }

    @Override
    public void applyTo( InFlightCache inFlightCache, Log log ) throws IOException
    {
        // only the actual log prunes
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        PruneLogCommand that = (PruneLogCommand) o;
        return pruneIndex == that.pruneIndex;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( pruneIndex );
    }

    @Override
    public String toString()
    {
        return "PruneLogCommand{" +
               "pruneIndex=" + pruneIndex +
               '}';
    }
}
