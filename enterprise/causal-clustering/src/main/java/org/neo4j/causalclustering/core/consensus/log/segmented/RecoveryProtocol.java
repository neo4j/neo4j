/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.neo4j.causalclustering.core.consensus.log.EntryRecord;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
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
    private ReaderPool readerPool;

    RecoveryProtocol( FileSystemAbstraction fileSystem, FileNames fileNames, ReaderPool readerPool,
            ChannelMarshal<ReplicatedContent> contentMarshal, LogProvider logProvider )
    {
        this.fileSystem = fileSystem;
        this.fileNames = fileNames;
        this.readerPool = readerPool;
        this.contentMarshal = contentMarshal;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    State run() throws IOException, DamagedLogStorageException, DisposedException
    {
        State state = new State();
        SortedMap<Long,File> files = fileNames.getAllFiles( fileSystem, log );

        if ( files.entrySet().isEmpty() )
        {
            state.segments = new Segments( fileSystem, fileNames, readerPool, emptyList(), contentMarshal, logProvider, -1 );
            state.segments.rotate( -1, -1, -1 );
            state.terms = new Terms( -1, -1 );
            return state;
        }

        List<SegmentFile> segmentFiles = new ArrayList<>();
        SegmentFile segment = null;

        long expectedVersion = files.firstKey();
        boolean mustRecoverLastHeader = false;
        boolean skip = true; // the first file is treated the same as a skip

        for ( Map.Entry<Long,File> entry : files.entrySet() )
        {
            long fileNameVersion = entry.getKey();
            File file = entry.getValue();
            SegmentHeader header;

            checkVersionSequence( fileNameVersion, expectedVersion );

            try
            {
                header = loadHeader( fileSystem, file );
                checkVersionMatches( header.version(), fileNameVersion );
            }
            catch ( EndOfStreamException e )
            {
                if ( files.lastKey() != fileNameVersion )
                {
                    throw new DamagedLogStorageException( e, "Intermediate file with incomplete or no header found: %s", file );
                }
                else if ( files.size() == 1 )
                {
                    throw new DamagedLogStorageException( e, "Single file with incomplete or no header found: %s", file );
                }

                /* Last file header must be recovered by scanning next-to-last file and writing a new header based on that. */
                mustRecoverLastHeader = true;
                break;
            }

            segment = new SegmentFile( fileSystem, file, readerPool, fileNameVersion, contentMarshal, logProvider, header );
            segmentFiles.add( segment );

            if ( segment.header().prevIndex() != segment.header().prevFileLastIndex() )
            {
                log.info( format( "Skipping from index %d to %d.", segment.header().prevFileLastIndex(),
                        segment.header().prevIndex() + 1 ) );
                skip = true;
            }

            if ( skip )
            {
                state.prevIndex = segment.header().prevIndex();
                state.prevTerm = segment.header().prevTerm();
                skip = false;
            }

            expectedVersion++;
        }

        assert segment != null;

        state.appendIndex = segment.header().prevIndex();
        state.terms = new Terms( segment.header().prevIndex(), segment.header().prevTerm() );

        try ( IOCursor<EntryRecord> cursor = segment.getCursor( segment.header().prevIndex() + 1 ) )
        {
            while ( cursor.next() )
            {
                EntryRecord entry = cursor.get();
                state.appendIndex = entry.logIndex();
                state.terms.append( state.appendIndex, entry.logEntry().term() );
            }
        }

        if ( mustRecoverLastHeader )
        {
            SegmentHeader header = new SegmentHeader( state.appendIndex, expectedVersion, state.appendIndex, state.terms.latest() );
            log.warn( "Recovering last file based on next-to-last file. " + header );

            File file = fileNames.getForVersion( expectedVersion );
            writeHeader( fileSystem, file, header );

            segment = new SegmentFile( fileSystem, file, readerPool, expectedVersion, contentMarshal, logProvider, header );
            segmentFiles.add( segment );
        }

        state.segments = new Segments( fileSystem, fileNames, readerPool, segmentFiles, contentMarshal, logProvider,
                segment.header().version() );

        return state;
    }

    private static SegmentHeader loadHeader(
            FileSystemAbstraction fileSystem,
            File file ) throws IOException, EndOfStreamException
    {
        try ( StoreChannel channel = fileSystem.open( file, OpenMode.READ ) )
        {
            return headerMarshal.unmarshal( new ReadAheadChannel<>( channel, SegmentHeader.SIZE ) );
        }
    }

    private static void writeHeader(
            FileSystemAbstraction fileSystem,
            File file,
            SegmentHeader header ) throws IOException
    {
        try ( StoreChannel channel = fileSystem.open( file, OpenMode.READ_WRITE ) )
        {
            channel.position( 0 );
            PhysicalFlushableChannel writer = new PhysicalFlushableChannel( channel, SegmentHeader.SIZE );
            headerMarshal.marshal( header, writer );
            writer.prepareForFlush().flush();
        }
    }

    private static void checkVersionSequence( long fileNameVersion, long expectedVersion ) throws DamagedLogStorageException
    {
        if ( fileNameVersion != expectedVersion )
        {
            throw new DamagedLogStorageException( "File versions not strictly monotonic. Expected: %d but found: %d",
                    expectedVersion, fileNameVersion );
        }
    }

    private static void checkVersionMatches( long headerVersion, long fileNameVersion ) throws DamagedLogStorageException
    {
        if ( headerVersion != fileNameVersion )
        {
            throw new DamagedLogStorageException(
                    "File version does not match header version. Expected: %d but found: %d", headerVersion, fileNameVersion );
        }
    }
}
