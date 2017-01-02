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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.EntryRecord;
import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;

class SegmentedRaftLogCursor implements RaftLogCursor
{
    private final IOCursor<EntryRecord> inner;
    private CursorValue<RaftLogEntry> current;
    private long index;

    SegmentedRaftLogCursor( long fromIndex, IOCursor<EntryRecord> inner )
    {
        this.inner = inner;
        this.current = new CursorValue<>();
        this.index = fromIndex - 1;
    }

    @Override
    public boolean next() throws IOException
    {
        boolean hasNext = inner.next();
        if ( hasNext )
        {
            current.set( inner.get().logEntry() );
            index++;
        }
        else
        {
            current.invalidate();
        }
        return hasNext;
    }

    @Override
    public void close() throws IOException
    {
        inner.close();
    }

    @Override
    public long index()
    {
        return index;
    }

    @Override
    public RaftLogEntry get()
    {
        return current.get();
    }
}
