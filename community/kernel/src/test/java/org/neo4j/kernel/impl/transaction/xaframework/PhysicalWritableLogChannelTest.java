/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PhysicalWritableLogChannelTest
{
    @Test
    public void shouldWriteThroughRotation() throws Exception
    {
        // GIVEN
        final File firstFile = new File( directory.directory(), "file1" );
        final File secondFile = new File( directory.directory(), "file2" );
        LogVersionBridge bridge = new LogVersionBridge()
        {
            @Override
            public VersionedStoreChannel next( VersionedStoreChannel channel ) throws IOException
            {   // In this test scenario always rotate
                channel.close();
                return new PhysicalLogVersionedStoreChannel( fs.open( secondFile, "rw" ), 2 );
            }
        };
        WritableLogChannel channel = new PhysicalWritableLogChannel(
                new PhysicalLogVersionedStoreChannel( fs.open( firstFile, "rw" ), 1 ), bridge );

        // WHEN writing a transaction, of sorts
        byte byteValue = (byte) 4;
        short shortValue = (short) 10;
        int intValue = 3545;
        long longValue = 45849589L;
        float floatValue = 45849.332f;
        double doubleValue = 458493343D;
        byte[] byteArrayValue = new byte[] {1,4,2,5,3,6};
        char[] charArrayValue = new char[] {'4', '5', '6', '7'};

        channel.put( byteValue );
        channel.putShort( shortValue );
        channel.putInt( intValue );
        channel.putLong( longValue );

        // and WHEN forcing -- effectively rotating the log in this test
        channel.force();
        channel.putFloat( floatValue );
        channel.putDouble( doubleValue );
        channel.put( byteArrayValue, byteArrayValue.length );
        channel.put( charArrayValue, charArrayValue.length );
        channel.close();

        // The two chunks of values should end up in two different files
        ByteBuffer firstFileContents = readFile( firstFile );
        assertEquals( byteValue, firstFileContents.get() );
        assertEquals( shortValue, firstFileContents.getShort() );
        assertEquals( intValue, firstFileContents.getInt() );
        assertEquals( longValue, firstFileContents.getLong() );
        ByteBuffer secondFileContents = readFile( secondFile );
        assertEquals( floatValue, secondFileContents.getFloat(), 0.0f );
        assertEquals( doubleValue, secondFileContents.getDouble(), 0.0d );

        byte[] readByteArray = new byte[byteArrayValue.length];
        secondFileContents.get( readByteArray );
        assertArrayEquals( byteArrayValue, readByteArray );

        char[] readCharArray = new char[charArrayValue.length];
        secondFileContents.asCharBuffer().get( readCharArray );
        assertArrayEquals( charArrayValue, readCharArray );
    }

    private ByteBuffer readFile( File file ) throws IOException
    {
        try ( StoreChannel channel = fs.open( file, "r" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( (int) channel.size() );
            channel.read( buffer );
            buffer.flip();
            return buffer;
        }
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
}
