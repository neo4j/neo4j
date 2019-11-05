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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChecksumChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.fs.ChecksumWriter.CHECKSUM_FACTORY;

@SuppressWarnings( "ResultOfMethodCallIgnored" )
@TestDirectoryExtension
class PhysicalFlushableChecksumChannelTest
{
    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory directory;

    @Test
    void calculateChecksum() throws IOException
    {
        final File firstFile = new File( directory.homeDir(), "file1" );
        StoreChannel storeChannel = fileSystem.write( firstFile );
        int channelChecksum;
        try ( PhysicalFlushableChecksumChannel channel = new PhysicalFlushableChecksumChannel( storeChannel ) )
        {
            channel.beginChecksum();
            channel.put( (byte) 10 );
            channelChecksum = channel.putChecksum();
        }

        int fileSize = (int) fileSystem.getFileSize( firstFile );
        assertEquals( Byte.BYTES + Integer.BYTES, fileSize );
        byte[] writtenBytes = new byte[fileSize];
        try ( InputStream in = new FileInputStream( firstFile ) )
        {
            in.read( writtenBytes );
        }
        ByteBuffer buffer = ByteBuffer.wrap( writtenBytes );

        Checksum checksum = CHECKSUM_FACTORY.get();
        checksum.update( 10 );

        assertEquals( checksum.getValue(), channelChecksum );
        assertEquals( 10, buffer.get() );
        assertEquals( checksum.getValue(), buffer.getInt() );
    }

    @Test
    void beginCehcksumShouldResetCalculations() throws IOException
    {
        final File firstFile = new File( directory.homeDir(), "file1" );
        StoreChannel storeChannel = fileSystem.write( firstFile );
        int channelChecksum;
        try ( PhysicalFlushableChecksumChannel channel = new PhysicalFlushableChecksumChannel( storeChannel ) )
        {
            channel.put( (byte) 5 );
            channel.beginChecksum();
            channel.put( (byte) 10 );
            channelChecksum = channel.putChecksum();
        }

        int fileSize = (int) fileSystem.getFileSize( firstFile );
        assertEquals( Byte.BYTES + Byte.BYTES + Integer.BYTES, fileSize );
        byte[] writtenBytes = new byte[fileSize];
        try ( InputStream in = new FileInputStream( firstFile ) )
        {
            in.read( writtenBytes );
        }
        ByteBuffer buffer = ByteBuffer.wrap( writtenBytes );

        Checksum checksum = CHECKSUM_FACTORY.get();
        checksum.update( 10 );

        assertEquals( checksum.getValue(), channelChecksum );
        assertEquals( 5, buffer.get() );
        assertEquals( 10, buffer.get() );
        assertEquals( checksum.getValue(), buffer.getInt() );
    }
}
