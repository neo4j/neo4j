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
package org.neo4j.coreedge.raft.log.segmented;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.neo4j.coreedge.raft.log.EntryRecord;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.raft.state.UnexpectedEndOfStreamException;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;

/**
 * Recovers all the state required for operating the RAFT log and does some simple
 * verifications; e.g. checking for gaps, verifying headers.
 */
class RecoveryProtocol
{
    private static final SegmentHeader.Marshal headerMarshal = new SegmentHeader.Marshal();

    private final FileSystemAbstraction fileSystem;
    private final FileNames fileNames;
    private final ChannelMarshal<ReplicatedContent> contentMarshal;
    private final LogProvider logProvider;
    private final Log log;
    private long expectedVersion;

    RecoveryProtocol( FileSystemAbstraction fileSystem, FileNames fileNames,
            ChannelMarshal<ReplicatedContent> contentMarshal, LogProvider logProvider )
    {
        this.fileSystem = fileSystem;
        this.fileNames = fileNames;
        this.contentMarshal = contentMarshal;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    State run() throws IOException, DamagedLogStorageException
    {
        State state = new State();
        SortedMap<Long,File> files = fileNames.getAllFiles( fileSystem, log );

        if ( files.entrySet().isEmpty() )
        {
            state.segments = new Segments( fileSystem, fileNames, emptyList(), contentMarshal, logProvider, -1 );
            state.segments.rotate( -1, -1, -1 );
            return state;
        }

        List<SegmentFile> segmentFiles = new ArrayList<>();
        long firstVersion = files.firstKey();
        expectedVersion = firstVersion;

        for ( Map.Entry<Long,File> entry : files.entrySet() )
        {
            try
            {
                long fileNameVersion = entry.getKey();
                File file = entry.getValue();

                SegmentHeader header;
                try
                {
                    header = loadHeader( fileSystem, file );
                }
                catch ( UnexpectedEndOfStreamException e )
                {
                    if ( files.lastKey() != fileNameVersion )
                    {
                        throw new DamagedLogStorageException( e, "File with incomplete or no header found: %s", file );
                    }

                    header = new SegmentHeader( state.appendIndex, fileNameVersion, state.appendIndex, state.currentTerm );
                    writeHeader( fileSystem, file, header );
                }

                SegmentFile segment = new SegmentFile( fileSystem, file, contentMarshal, logProvider, header );

                checkVersionStrictlyMonotonic( fileNameVersion );
                checkVersionMatches( segment.header().version(), fileNameVersion );

                segmentFiles.add( segment );

                if ( fileNameVersion == firstVersion )
                {
                    state.prevIndex = segment.header().prevIndex();
                    state.prevTerm = segment.header().prevTerm();
                }

                expectedVersion++;
                // check term
            }
            catch ( IOException e )
            {
                log.error( "Error during recovery", e );
            }
        }

        SegmentFile last = segmentFiles.get( segmentFiles.size() - 1 );

        state.segments = new Segments( fileSystem, fileNames, segmentFiles, contentMarshal, logProvider, files.lastKey() );
        state.appendIndex = last.header().prevIndex();
        state.currentTerm = last.header().prevTerm();

        long firstIndexInLastSegmentFile = last.header().prevIndex() + 1;
        try ( IOCursor<EntryRecord> reader =  last.getReader( firstIndexInLastSegmentFile ) )
        {
            while ( reader.next() )
            {
                EntryRecord entry = reader.get();
                state.appendIndex = entry.logIndex();
                state.currentTerm = entry.logEntry().term();
            }
        }
        catch ( DisposedException e )
        {
            throw new RuntimeException( "Unexpected exception", e );
        }

        return state;
    }

    private static SegmentHeader loadHeader(
            FileSystemAbstraction fileSystem,
            File file ) throws IOException, UnexpectedEndOfStreamException
    {
        try ( StoreChannel channel = fileSystem.open( file, "r" ) )
        {
            return headerMarshal.unmarshal( new ReadAheadChannel<>( channel, SegmentHeader.SIZE ) );
        }
    }

    private static void writeHeader(
            FileSystemAbstraction fileSystem,
            File file,
            SegmentHeader header ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.open( file, "rw" ) )
        {
            channel.position( 0 );
            PhysicalFlushableChannel writer = new PhysicalFlushableChannel( channel, SegmentHeader.SIZE );
            headerMarshal.marshal( header, writer );
            writer.prepareForFlush().flush();
        }
    }

    private void checkVersionStrictlyMonotonic( long fileNameVersion ) throws DamagedLogStorageException
    {
        if ( fileNameVersion != expectedVersion )
        {
            throw new DamagedLogStorageException( "File versions not strictly monotonic. Expected: %d but found: %d", expectedVersion, fileNameVersion );
        }
    }

    private void checkVersionMatches( long headerVersion, long fileNameVersion ) throws DamagedLogStorageException
    {
        if ( headerVersion != fileNameVersion )
        {
            throw new DamagedLogStorageException( "File version does not match header version. Expected: %d but found: %d", headerVersion, fileNameVersion );
        }
    }
}
