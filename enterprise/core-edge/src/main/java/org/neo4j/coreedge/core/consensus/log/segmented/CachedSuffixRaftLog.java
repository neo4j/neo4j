/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core.consensus.log.segmented;

import java.io.IOException;

import org.neo4j.coreedge.core.consensus.log.RaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLogCursor;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;

public class CachedSuffixRaftLog implements RaftLog
{
    private final RaftLog fullLog;
    private final RaftLog cacheLog;

    public CachedSuffixRaftLog( RaftLog fullLog, RaftLog cacheLog )
    {
        this.fullLog = fullLog;
        this.cacheLog = cacheLog;
    }

    @Override
    public long append( RaftLogEntry... entries ) throws IOException
    {
        long inFlightAppendIndex = cacheLog.append( entries );
        long fullAppendIndex = fullLog.append( entries );
        assert inFlightAppendIndex == fullAppendIndex;
        return inFlightAppendIndex;
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        cacheLog.truncate( fromIndex );
        fullLog.truncate( fromIndex );
    }

    @Override
    public long prune( long safeIndex ) throws IOException
    {
        cacheLog.prune( safeIndex );
        return fullLog.prune( safeIndex );
    }

    @Override
    public long skip( long index, long term ) throws IOException
    {
        long inFlightAppendIndex = cacheLog.skip( index, term );
        long fullAppendIndex = fullLog.skip( index, term );
        assert inFlightAppendIndex == fullAppendIndex;
        return inFlightAppendIndex;
    }

    @Override
    public long appendIndex()
    {
        return fullLog.appendIndex();
    }

    @Override
    public long prevIndex()
    {
        return fullLog.prevIndex();
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        if ( logIndex > cacheLog.prevIndex() )
        {
            return cacheLog.readEntryTerm( logIndex );
        }
        return fullLog.readEntryTerm( logIndex );
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
    {
        if ( fromIndex > cacheLog.prevIndex() )
        {
            return cacheLog.getEntryCursor( fromIndex );
        }
        return fullLog.getEntryCursor( fromIndex );
    }
}
