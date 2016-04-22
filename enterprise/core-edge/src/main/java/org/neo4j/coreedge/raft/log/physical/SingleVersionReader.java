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
package org.neo4j.coreedge.raft.log.physical;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.log.RaftAppendRecordCursor;
import org.neo4j.coreedge.raft.log.RaftLogAppendRecord;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

public class SingleVersionReader
{
    private final PhysicalRaftLogFiles files;
    private final FileSystemAbstraction fileSystem;
    private final ChannelMarshal<ReplicatedContent> marshal;

    public SingleVersionReader( PhysicalRaftLogFiles files, FileSystemAbstraction fileSystem,
                                 ChannelMarshal<ReplicatedContent> marshal )
    {
        this.files = files;
        this.fileSystem = fileSystem;
        this.marshal = marshal;
    }

    public IOCursor<RaftLogAppendRecord> readEntriesFrom( LogPosition position ) throws IOException
    {
        File file = files.getLogFileForVersion( position.getLogVersion() );

        StoreChannel rawChannel = fileSystem.open( file, "rw" );
        ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );
        LogHeader header = readLogHeader( buffer, rawChannel, true );
        assert header != null && header.logVersion == position.getLogVersion();

        PhysicalLogVersionedStoreChannel physicalLogVersionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( rawChannel, position.getLogVersion(), header.logFormatVersion );
        physicalLogVersionedStoreChannel.position( position.getByteOffset() );
        ReadAheadChannel<LogVersionedStoreChannel> readAheadChannel = new ReadAheadChannel<>(
                physicalLogVersionedStoreChannel );

        return new RaftAppendRecordCursor( readAheadChannel, marshal );
    }
}
