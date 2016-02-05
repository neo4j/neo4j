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
package org.neo4j.coreedge.raft.log;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.function.Predicate;

import org.neo4j.cursor.IOCursor;

public class PhysicalRaftLogEntryCursor implements IOCursor<RaftLogAppendRecord>
{
    private final RaftRecordCursor<?> recordCursor;
    private RaftLogAppendRecord currentEntry;
    private final Queue<RaftLogAppendRecord> logicallyNext;
    private final List<RaftLogAppendRecord> staging;

    public PhysicalRaftLogEntryCursor( RaftRecordCursor<?> recordCursor )
    {
        this.recordCursor = recordCursor;
        this.logicallyNext = new LinkedList<>();
        this.staging = new LinkedList<>();
    }

    @Override
    public boolean next() throws IOException
    {
        while ( recordCursor.next() )
        {
            RaftLogRecord record = recordCursor.get();
            switch ( record.getType() )
            {
                case APPEND:
                    staging.add( (RaftLogAppendRecord) record );
                    break;
                case COMMIT:
                {
                    moveStagingToLogicallyNextAndSetCurrentEntry( logIndex -> logIndex <= record.getLogIndex() );

                    if ( currentEntry != null )
                    {
                        return true;
                    }

                    break;
                }
                case TRUNCATE:
                {
                    removeFromStaging( index -> index >= record.getLogIndex() );
                    break;
                }
            }
        }
        moveStagingToLogicallyNextAndSetCurrentEntry( index -> true );
        return currentEntry != null;
    }

    private void removeFromStaging( Predicate<Long> condition )
    {
        ListIterator<RaftLogAppendRecord> iterator = staging.listIterator();
        while ( iterator.hasNext() )
        {
            if ( condition.test( iterator.next().getLogIndex() ) )
            {
                iterator.remove();
            }
        }
    }

    private void moveStagingToLogicallyNextAndSetCurrentEntry( Predicate<Long> condition )
    {
        ListIterator<RaftLogAppendRecord> iterator = staging.listIterator();
        while ( iterator.hasNext() )
        {
            RaftLogAppendRecord next = iterator.next();
            if ( condition.test( next.getLogIndex() ) )
            {
                logicallyNext.offer( next );
                iterator.remove();
            }
        }
        currentEntry = logicallyNext.poll();
    }

    @Override
    public void close() throws IOException
    {
        recordCursor.close();
    }

    @Override
    public RaftLogAppendRecord get()
    {
        return currentEntry;
    }
}
