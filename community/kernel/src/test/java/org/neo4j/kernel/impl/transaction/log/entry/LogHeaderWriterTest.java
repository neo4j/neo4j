/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.decodeLogFormatVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.decodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.encodeLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class LogHeaderWriterTest
{
    private final long expectedLogVersion = CURRENT_LOG_VERSION;
    private final long expectedTxId = 42;

    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldWriteALogHeaderInTheGivenChannel() throws IOException
    {
        // given
        final InMemoryClosableChannel channel = new InMemoryClosableChannel();

        // when
        writeLogHeader( channel, expectedLogVersion, expectedTxId );

        // then
        long encodedLogVersions = channel.getLong();
        assertEquals( encodeLogVersion( expectedLogVersion ), encodedLogVersions );

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        assertEquals( CURRENT_LOG_VERSION, logFormatVersion );

        long logVersion = decodeLogVersion( encodedLogVersions );
        assertEquals( expectedLogVersion, logVersion );

        long txId = channel.getLong();
        assertEquals( expectedTxId, txId );
    }

    @Test
    void shouldWriteALogHeaderInTheGivenBuffer()
    {
        // given
        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );

        // when
        final ByteBuffer result = writeLogHeader( buffer, expectedLogVersion, expectedTxId );

        // then
        assertSame( buffer, result );

        long encodedLogVersions = result.getLong();
        assertEquals( encodeLogVersion( expectedLogVersion ), encodedLogVersions );

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        assertEquals( CURRENT_LOG_VERSION, logFormatVersion );

        long logVersion = decodeLogVersion( encodedLogVersions );
        assertEquals( expectedLogVersion, logVersion );

        long txId = result.getLong();
        assertEquals( expectedTxId, txId );
    }

    @Test
    void shouldWriteALogHeaderInAFile() throws IOException
    {
        // given
        final File file = testDirectory.file( "WriteLogHeader" );

        // when
        writeLogHeader( fileSystem, file, expectedLogVersion, expectedTxId );

        // then
        final byte[] array = new byte[LOG_HEADER_SIZE];
        try ( InputStream stream = fileSystem.openAsInputStream( file ) )
        {
            int read = stream.read( array );
            assertEquals( LOG_HEADER_SIZE, read );
        }
        final ByteBuffer result = ByteBuffer.wrap( array );

        long encodedLogVersions = result.getLong();
        assertEquals( encodeLogVersion( expectedLogVersion ), encodedLogVersions );

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        assertEquals( CURRENT_LOG_VERSION, logFormatVersion );

        long logVersion = decodeLogVersion( encodedLogVersions );
        assertEquals( expectedLogVersion, logVersion );

        long txId = result.getLong();
        assertEquals( expectedTxId, txId );
    }

    @Test
    void shouldWriteALogHeaderInAStoreChannel() throws IOException
    {
        // given
        final File file = testDirectory.file( "WriteLogHeader" );
        final StoreChannel channel = fileSystem.open( file, OpenMode.READ_WRITE );

        // when
        writeLogHeader( channel, expectedLogVersion, expectedTxId );

        channel.close();

        // then
        final byte[] array = new byte[LOG_HEADER_SIZE];
        try ( InputStream stream = fileSystem.openAsInputStream( file ) )
        {
            int read = stream.read( array );
            assertEquals( LOG_HEADER_SIZE, read );
        }
        final ByteBuffer result = ByteBuffer.wrap( array );

        long encodedLogVersions = result.getLong();
        assertEquals( encodeLogVersion( expectedLogVersion ), encodedLogVersions );

        byte logFormatVersion = decodeLogFormatVersion( encodedLogVersions );
        assertEquals( CURRENT_LOG_VERSION, logFormatVersion );

        long logVersion = decodeLogVersion( encodedLogVersions );
        assertEquals( expectedLogVersion, logVersion );

        long txId = result.getLong();
        assertEquals( expectedTxId, txId );
    }
}
