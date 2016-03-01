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

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogHeaderVisitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

class PhysicalRaftEntryStore implements RaftEntryStore
{
    private final LogFile logFile;
    private final RaftLogMetadataCache metadataCache;
    private final ChannelMarshal<ReplicatedContent> marshal;

    PhysicalRaftEntryStore( LogFile logFile, RaftLogMetadataCache metadataCache,
            ChannelMarshal<ReplicatedContent> marshal )
    {
        this.logFile = logFile;
        this.metadataCache = metadataCache;
        this.marshal = marshal;
    }

    @Override
    public IOCursor<RaftLogAppendRecord> getEntriesFrom( long fromIndex ) throws IOException
    {
        // generate skip stack and get starting position
        Stack<Long> skipStack = new Stack<>();
        SkipStackGenerator skipStackGenerator = new SkipStackGenerator( fromIndex, skipStack );
        logFile.accept( skipStackGenerator );

        // the skip stack generator scans through the headers and gives us the logs starting position as a side-effect
        LogPosition startPosition = skipStackGenerator.logStartPosition;

        if ( startPosition == null )
        {
            return IOCursor.getEmpty();
        }

        RaftLogMetadataCache.RaftLogEntryMetadata logEntryInfo = metadataCache.getMetadata( fromIndex );
        if( logEntryInfo != null && logEntryInfo.getStartPosition().getLogVersion() == startPosition.getLogVersion() )
        {
            // then metadata is valid for this log version, read from there
            startPosition = logEntryInfo.getStartPosition();
        }

        return new PhysicalRaftLogEntryCursor( new RaftRecordCursor<>( logFile.getReader( startPosition ), marshal ),
                skipStack, fromIndex );
    }

    private static final class SkipStackGenerator implements LogHeaderVisitor
    {
        private final long logEntryIndex;
        private final Stack<Long> skipStack;
        private LogPosition logStartPosition;
        private long nextContinuation = -1;

        private SkipStackGenerator( long logEntryIndex, Stack<Long> skipStack )
        {
            this.logEntryIndex = logEntryIndex;
            this.skipStack = skipStack;
        }

        /**
         * Visits all log files backwards in order and creates a stack defining where a record traversal
         * should skip forward to the next continuation record. */
        @Override
        public boolean visit( LogPosition position, long firstLogIndex, long ignored )
        {
            if( nextContinuation != -1 )
            {
                if( !skipStack.empty() && skipStack.peek() < nextContinuation )
                {
                    // This happens if you truncate again past a previous truncation. For example
                    // truncating to 7 first and later truncating to 3. Thus the older truncation becomes
                    // irrelevant and must be ignored.
                    //
                    // So one must do a double-skip to reach the start of the latest 3 and this is
                    // implemented by setting the older skip point to the value of the newer one. Thus
                    // instead of skipping to 7 and then to 3, we will skip to 3 and again to 3.
                    //
                    // The skip points mark the indexes where we should start skipping from until the next
                    // continuation record. Thus if we skip starting from 3, reach the continuation record,
                    // and yet again find that we should skip to 3 (by popping the skip stack) then we will
                    // simply keep skipping.
                    nextContinuation = skipStack.peek();
                }
                skipStack.push( nextContinuation );
            }

            if ( logEntryIndex >= firstLogIndex )
            {
                logStartPosition = position;
                return false;
            }

            nextContinuation = firstLogIndex;
            return true;
        }
    }
}
