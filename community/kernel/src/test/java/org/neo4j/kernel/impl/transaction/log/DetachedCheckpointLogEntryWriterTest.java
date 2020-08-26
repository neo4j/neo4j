/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChecksumChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.transaction.log.DetachedCheckpointLogEntryWriter.RECORD_LENGTH_BYTES;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@TestDirectoryExtension
class DetachedCheckpointLogEntryWriterTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;

    @Test
    void detachedCheckpointEntryHasSpecificLength() throws IOException
    {
        try ( var buffer = new HeapScopedBuffer( (int) kibiBytes( 1 ), INSTANCE ) )
        {
            StoreChannel storeChannel = fs.write( directory.createFilePath( "a" ) );
            try ( PhysicalFlushableChecksumChannel writeChannel = new PhysicalFlushableChecksumChannel( storeChannel, buffer ) )
            {
                var checkpointLogEntryWriter = new DetachedCheckpointLogEntryWriter( writeChannel );
                long initialPosition = writeChannel.position();
                writeCheckpoint( checkpointLogEntryWriter, "checkpoint reason" );

                assertThat( writeChannel.position() - initialPosition ).isEqualTo( RECORD_LENGTH_BYTES );
            }
        }
    }

    @Test
    void anyCheckpointEntryHaveTheSameSize() throws IOException
    {
        try ( var buffer = new HeapScopedBuffer( (int) kibiBytes( 1 ), INSTANCE ) )
        {
            StoreChannel storeChannel = fs.write( directory.createFilePath( "b" ) );
            try ( PhysicalFlushableChecksumChannel writeChannel = new PhysicalFlushableChecksumChannel( storeChannel, buffer ) )
            {
                var checkpointLogEntryWriter = new DetachedCheckpointLogEntryWriter( writeChannel );

                for ( int i = 0; i < 100; i++ )
                {
                    long initialPosition = writeChannel.position();
                    writeCheckpoint( checkpointLogEntryWriter, randomAlphabetic( 10, 512 ) );
                    long recordLength = writeChannel.position() - initialPosition;
                    assertThat( recordLength ).isEqualTo( RECORD_LENGTH_BYTES );
                }
            }
        }
    }

    @Test
    void longCheckpointReasonIsTrimmedToFit() throws IOException
    {
        try ( var buffer = new HeapScopedBuffer( (int) kibiBytes( 1 ), INSTANCE ) )
        {
            StoreChannel storeChannel = fs.write( directory.createFilePath( "b" ) );
            try ( PhysicalFlushableChecksumChannel writeChannel = new PhysicalFlushableChecksumChannel( storeChannel, buffer ) )
            {
                var checkpointLogEntryWriter = new DetachedCheckpointLogEntryWriter( writeChannel );

                long initialPosition = writeChannel.position();
                writeCheckpoint( checkpointLogEntryWriter, StringUtils.repeat( "b", 1024 ) );
                long recordLength = writeChannel.position() - initialPosition;
                assertThat( recordLength ).isEqualTo( RECORD_LENGTH_BYTES );

            }
        }
    }

    private static void writeCheckpoint( DetachedCheckpointLogEntryWriter checkpointLogEntryWriter, String reason ) throws IOException
    {
        var storeId = new StoreId( 3, 4, 5, 6, 7 );

        LogPosition logPosition = new LogPosition( 1, 2 );

        checkpointLogEntryWriter.writeCheckPointEntry( logPosition, Instant.ofEpochMilli( 1 ), storeId, reason );
    }
}
