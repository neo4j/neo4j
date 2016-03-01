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
import java.util.Stack;

import org.neo4j.cursor.IOCursor;

public class PhysicalRaftLogEntryCursor implements IOCursor<RaftLogAppendRecord>
{
    private long NO_SKIP = -1;
    private final RaftRecordCursor<?> recordCursor;

    private final Stack<Long> skipStack;
    private RaftLogAppendRecord currentEntry;
    private long nextIndex;
    private long skipPoint = NO_SKIP;
    private boolean skipMode = false;

    public PhysicalRaftLogEntryCursor( RaftRecordCursor<?> recordCursor, Stack<Long> skipStack, long fromIndex )
    {
        this.recordCursor = recordCursor;
        this.skipStack = skipStack;
        this.nextIndex = fromIndex;
        popSkip();
    }

    private void popSkip()
    {
        skipPoint = skipStack.empty() ? NO_SKIP : skipStack.pop();
    }

    @Override
    public boolean next() throws IOException
    {
        RaftLogRecord record;
        while ( recordCursor.next() )
        {
            record = recordCursor.get();
            switch ( record.getType() )
            {
                case APPEND:
                    if( skipMode )
                    {
                        // skip records
                    }
                    else if( record.getLogIndex() == nextIndex )
                    {
                        currentEntry = (RaftLogAppendRecord) record;

                        nextIndex++;
                        if( nextIndex == skipPoint )
                        {
                            skipMode = true;
                        }
                        return true;
                    }
                    break;
                case CONTINUATION:
                {
                    if( skipMode )
                    {
                        popSkip();
                        if( skipPoint == NO_SKIP || skipPoint > nextIndex )
                        {
                            skipMode = false;
                        }
                    }
                    break;
                }
            }
        }
        currentEntry = null;
        return false;
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
