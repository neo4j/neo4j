/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.entry;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChecksumChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT;
import static org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor.EMPTY_ACCESSOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
class DetachedCheckpointLogEntryParserTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;

    private final CommandReaderFactory commandReader = new TestCommandReaderFactory();
    private final LogPositionMarker positionMarker = new LogPositionMarker();

    @Test
    void parseDetachedCheckpointRecord() throws IOException
    {
        KernelVersion version = KernelVersion.V4_3_D4;
        var storeId = new StoreId( 4, 5, 6, 7, 8 );
        var channel = new InMemoryClosableChannel();
        int checkpointMillis = 3;
        String checkpointDescription = "checkpoint";
        byte[] bytes = Arrays.copyOf( checkpointDescription.getBytes(), 120 );
        // For version confusion, please read LogEntryParserSetV4_3 comments
        var checkpoint = new LogEntryDetachedCheckpoint( version, new LogPosition( 1, 2 ), checkpointMillis, storeId, checkpointDescription );

        channel.putLong( checkpoint.getLogPosition().getLogVersion() )
               .putLong( checkpoint.getLogPosition().getByteOffset() )
               .putLong( checkpointMillis )
               .putLong( storeId.getCreationTime() )
               .putLong( storeId.getRandomId() )
               .putLong( storeId.getStoreVersion() )
               .putLong( storeId.getUpgradeTime() )
               .putLong( storeId.getUpgradeTxId() )
               .putShort( (short) checkpointDescription.getBytes().length )
               .put( bytes, bytes.length );
        channel.putChecksum();

        var checkpointParser = LogEntryParserSets.parserSet( version ).select( DETACHED_CHECK_POINT );
        LogEntry logEntry = checkpointParser.parse( version, channel, positionMarker, commandReader );
        assertEquals( checkpoint, logEntry );
    }

    @Test
    void writeAndParseCheckpointKernelVersion() throws IOException
    {
        try ( var buffer = new HeapScopedBuffer( (int) kibiBytes( 1 ), INSTANCE ) )
        {
            Path path = directory.createFile( "a" );
            StoreChannel storeChannel = fs.write( path );
            try ( PhysicalFlushableChecksumChannel writeChannel = new PhysicalFlushableChecksumChannel( storeChannel, buffer ) )
            {
                MutableKernelVersionRepository kernelVersionProvider = new MutableKernelVersionRepository();
                var checkpointLogEntryWriter = new DetachedCheckpointLogEntryWriter( writeChannel, kernelVersionProvider );

                kernelVersionProvider.setKernelVersion( KernelVersion.V4_4 );
                writeCheckpoint( checkpointLogEntryWriter, StringUtils.repeat( "b", 1024 ) );

                kernelVersionProvider.setKernelVersion( KernelVersion.V5_0 );
                writeCheckpoint( checkpointLogEntryWriter, StringUtils.repeat( "c", 1024 ) );
            }

            VersionAwareLogEntryReader entryReader = new VersionAwareLogEntryReader( StorageEngineFactory.defaultStorageEngine().commandReaderFactory() );
            try ( var readChannel = new ReadAheadLogChannel(
                    new PhysicalLogVersionedStoreChannel( fs.read( path ), -1 /* ignored */, (byte) -1, path, EMPTY_ACCESSOR, DatabaseTracer.NULL ),
                    NO_MORE_CHANNELS, INSTANCE ) )
            {
                assertEquals( KernelVersion.V4_4, readCheckpoint( entryReader, readChannel ).getVersion() );
                assertEquals( KernelVersion.V5_0, readCheckpoint( entryReader, readChannel ).getVersion() );
            }
        }
    }

    private LogEntryDetachedCheckpoint readCheckpoint( VersionAwareLogEntryReader entryReader, ReadAheadLogChannel readChannel ) throws IOException
    {
        return (LogEntryDetachedCheckpoint) entryReader.readLogEntry( readChannel );
    }

    private static void writeCheckpoint( DetachedCheckpointLogEntryWriter checkpointLogEntryWriter, String reason ) throws IOException
    {
        var storeId = new StoreId( 3, 4, 5, 6, 7 );

        LogPosition logPosition = new LogPosition( 1, 2 );

        checkpointLogEntryWriter.writeCheckPointEntry( logPosition, Instant.ofEpochMilli( 1 ), storeId, reason );
    }

    private static class MutableKernelVersionRepository implements KernelVersionRepository
    {
        private KernelVersion kernelVersion;

        @Override
        public KernelVersion kernelVersion()
        {
            return kernelVersion;
        }

        public void setKernelVersion( KernelVersion kernelVersion )
        {
            this.kernelVersion = kernelVersion;
        }
    }
}
