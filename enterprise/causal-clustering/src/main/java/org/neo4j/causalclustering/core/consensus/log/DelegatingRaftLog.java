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
package org.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

public class DelegatingRaftLog implements RaftLog
{
    private final RaftLog inner;

    public DelegatingRaftLog( RaftLog inner )
    {
        this.inner = inner;
    }

    @Override
    public long append( RaftLogEntry... entry ) throws IOException
    {
        return inner.append( entry );
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        inner.truncate( fromIndex );
    }

    @Override
    public long prune( long safeIndex ) throws IOException
    {
        return inner.prune( safeIndex );
    }

    @Override
    public long skip( long index, long term ) throws IOException
    {
        return inner.skip( index, term );
    }

    @Override
    public long appendIndex()
    {
        return inner.appendIndex();
    }

    @Override
    public long prevIndex()
    {
        return inner.prevIndex();
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        return inner.readEntryTerm( logIndex );
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
    {
        return inner.getEntryCursor( fromIndex );
    }
}
