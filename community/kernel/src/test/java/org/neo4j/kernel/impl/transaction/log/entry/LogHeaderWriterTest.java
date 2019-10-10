/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.decodeLogFormatVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.decodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.LOG_VERSION_MASK;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_FORMAT_VERSION;

@TestDirectoryExtension
@ExtendWith( RandomExtension.class )
class LogHeaderWriterTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private RandomRule random;

    private long expectedLogVersion;
    private long expectedTxId;
    private StoreId expectedStoreId;
    private LogHeader logHeader;

    @BeforeEach
    void setUp()
    {
        expectedLogVersion = random.nextLong( 0, LOG_VERSION_MASK );
        expectedTxId = random.nextLong( 0, Long.MAX_VALUE );
        expectedStoreId = new StoreId( random.nextLong(), random.nextLong(), random.nextLong(), random.nextLong(), random.nextLong() );
        logHeader = new LogHeader( expectedLogVersion, expectedTxId, expectedStoreId );
    }

    @Test
    void shouldWriteALogHeaderInTheGivenChannel() throws IOException
    {
        // given
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        // when
        writeLogHeader( channel, logHeader );

        // then
        long encodedLogVersions = channel.getLong();
        assertEquals( encodeLogVersion( expectedLogVersion, CURRENT_LOG_FORMAT_VERSION ), encodedLogVersions );

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        assertEquals( CURRENT_LOG_FORMAT_VERSION, logFormatVersion );

        long logVersion = decodeLogVersion( encodedLogVersions );
        assertEquals( expectedLogVersion, logVersion );

        long txId = channel.getLong();
        assertEquals( expectedTxId, txId );

        StoreId storeId = new StoreId( channel.getLong(), channel.getLong(), channel.getLong(), channel.getLong(), channel.getLong() );
        assertEquals( expectedStoreId, storeId );
    }

    @Test
    void shouldWriteALogHeaderInAStoreChannel() throws IOException
    {
        // given
        final File file = testDirectory.file( "WriteLogHeader" );
        final StoreChannel channel = fileSystem.write( file );

        // when
        writeLogHeader( channel, logHeader );

        channel.close();

        // then
        final byte[] array = new byte[CURRENT_FORMAT_LOG_HEADER_SIZE];
        try ( InputStream stream = fileSystem.openAsInputStream( file ) )
        {
            int read = stream.read( array );
            assertEquals( CURRENT_FORMAT_LOG_HEADER_SIZE, read );
        }
        final ByteBuffer result = ByteBuffer.wrap( array );

        long encodedLogVersions = result.getLong();
        assertEquals( encodeLogVersion( expectedLogVersion, CURRENT_LOG_FORMAT_VERSION ), encodedLogVersions );

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        assertEquals( CURRENT_LOG_FORMAT_VERSION, logFormatVersion );

        long logVersion = decodeLogVersion( encodedLogVersions );
        assertEquals( expectedLogVersion, logVersion );

        long txId = result.getLong();
        assertEquals( expectedTxId, txId );

        StoreId storeId = new StoreId( result.getLong(), result.getLong(), result.getLong(), result.getLong(), result.getLong() );
        assertEquals( expectedStoreId, storeId );
    }
}
