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

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogHeaderVisitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.logging.LogProvider;

public class PhysicalRaftEntryStore implements RaftEntryStore
{
    private final LogFile logFile;
    private final RaftLogMetadataCache metadataCache;
    private final ChannelMarshal<ReplicatedContent> marshal;

    public PhysicalRaftEntryStore( LogFile logFile, RaftLogMetadataCache metadataCache,
                                   ChannelMarshal<ReplicatedContent> marshal )
    {
        this.logFile = logFile;
        this.metadataCache = metadataCache;
        this.marshal = marshal;
    }

    @Override
    public IOCursor<RaftLogAppendRecord> getEntriesFrom( final long indexToStartFrom ) throws IOException

    {
        // look up in position cache
        RaftLogMetadataCache.RaftLogEntryMetadata metadata = metadataCache.getMetadata( indexToStartFrom );
        LogPosition startPosition = null;
        if ( metadata != null )
        {
            startPosition = metadata.getStartPosition();
        }
        else
        {
            // ask LogFile about the version it may be in
            LogVersionLocator headerVisitor = new LogVersionLocator( indexToStartFrom );
            logFile.accept( headerVisitor );
            startPosition = headerVisitor.getLogPosition();
        }
        if ( startPosition == null )
        {
            return IOCursor.getEmpty();
        }
        ReadableLogChannel reader = logFile.getReader( startPosition );

        return new PhysicalRaftLogEntryCursor( new RaftRecordCursor<>( reader, marshal ) );
    }

    public static final class LogVersionLocator implements LogHeaderVisitor
    {
        private final long logEntryIndex;
        private LogPosition foundPosition;

        public LogVersionLocator( long logEntryIndex )
        {
            this.logEntryIndex = logEntryIndex;
        }

        @Override
        public boolean visit( LogPosition position, long firstLogIndex, long lastLogIndex )
        {
            boolean foundIt = logEntryIndex >= firstLogIndex && logEntryIndex <= lastLogIndex;
            if ( foundIt )
            {
                foundPosition = position;
            }
            return !foundIt; // continue as long we don't find it
        }

        public LogPosition getLogPosition()
        {
            return foundPosition;
        }
    }
}
