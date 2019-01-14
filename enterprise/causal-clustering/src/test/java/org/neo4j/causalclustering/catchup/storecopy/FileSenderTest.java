/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBufAllocator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Random;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.storecopy.FileChunk.MAX_SIZE;

public class FileSenderTest
{
    private final Random random = new Random();
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final FileSystemAbstraction fs = fsRule.get();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fsRule.get() );
    private ByteBufAllocator allocator = mock( ByteBufAllocator.class );
    private PageCache pageCache = mock( PageCache.class );

    @Before
    public void setup() throws IOException
    {
        when( pageCache.getExistingMapping( any() ) ).thenReturn( Optional.empty() );
    }

    @Test
    public void sendEmptyFile() throws Exception
    {
        // given
        File emptyFile = testDirectory.file( "emptyFile" );
        fs.create( emptyFile ).close();
        FileSender fileSender = new FileSender( new StoreResource( emptyFile, null, 16, pageCache, fs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertEquals( FileChunk.create( new byte[0], true ), fileSender.readChunk( allocator ) );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    public void sendSmallFile() throws Exception
    {
        // given
        byte[] bytes = new byte[10];
        random.nextBytes( bytes );

        File smallFile = testDirectory.file( "smallFile" );
        try ( StoreChannel storeChannel = fs.create( smallFile ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }

        FileSender fileSender = new FileSender( new StoreResource( smallFile, null, 16, pageCache, fs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertEquals( FileChunk.create( bytes, true ), fileSender.readChunk( allocator ) );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    public void sendLargeFile() throws Exception
    {
        // given
        int dataSize = MAX_SIZE + (MAX_SIZE / 2);
        byte[] bytes = new byte[dataSize];
        random.nextBytes( bytes );

        File smallFile = testDirectory.file( "smallFile" );
        try ( StoreChannel storeChannel = fs.create( smallFile ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }

        FileSender fileSender = new FileSender( new StoreResource( smallFile, null, 16, pageCache, fs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertEquals( FileChunk.create( copyOfRange( bytes, 0, MAX_SIZE ), false ), fileSender.readChunk( allocator ) );
        assertEquals( FileChunk.create( copyOfRange( bytes, MAX_SIZE, bytes.length ), true ), fileSender.readChunk( allocator ) );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    public void sendLargeFileWithSizeMultipleOfTheChunkSize() throws Exception
    {
        // given
        byte[] bytes = new byte[MAX_SIZE * 3];
        random.nextBytes( bytes );

        File smallFile = testDirectory.file( "smallFile" );
        try ( StoreChannel storeChannel = fs.create( smallFile ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }

        FileSender fileSender = new FileSender( new StoreResource( smallFile, null, 16, pageCache, fs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertEquals( FileChunk.create( copyOfRange( bytes, 0, MAX_SIZE ), false ), fileSender.readChunk( allocator ) );
        assertEquals( FileChunk.create( copyOfRange( bytes, MAX_SIZE, MAX_SIZE * 2 ), false ), fileSender.readChunk( allocator ) );
        assertEquals( FileChunk.create( copyOfRange( bytes, MAX_SIZE * 2, bytes.length ), true ), fileSender.readChunk( allocator ) );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    @Test
    public void sendEmptyFileWhichGrowsBeforeSendCommences() throws Exception
    {
        // given
        File file = testDirectory.file( "file" );
        StoreChannel writer = fs.create( file );
        FileSender fileSender = new FileSender( new StoreResource( file, null, 16, pageCache, fs ) );

        // when
        byte[] bytes = writeRandomBytes( writer, 1024 );

        // then
        assertFalse( fileSender.isEndOfInput() );
        assertEquals( FileChunk.create( bytes, true ), fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    public void sendEmptyFileWhichGrowsWithPartialChunkSizes() throws Exception
    {
        // given
        File file = testDirectory.file( "file" );
        StoreChannel writer = fs.create( file );
        FileSender fileSender = new FileSender( new StoreResource( file, null, 16, pageCache, fs ) );

        // when
        byte[] chunkA = writeRandomBytes( writer, MAX_SIZE );
        byte[] chunkB = writeRandomBytes( writer, MAX_SIZE / 2 );

        // then
        assertEquals( FileChunk.create( chunkA, false ), fileSender.readChunk( allocator ) );
        assertFalse( fileSender.isEndOfInput() );

        // when
        writeRandomBytes( writer, MAX_SIZE / 2 );

        // then
        assertEquals( FileChunk.create( chunkB, true ), fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    public void sendFileWhichGrowsAfterLastChunkWasSent() throws Exception
    {
        // given
        File file = testDirectory.file( "file" );
        StoreChannel writer = fs.create( file );
        FileSender fileSender = new FileSender( new StoreResource( file, null, 16, pageCache, fs ) );

        // when
        byte[] chunkA = writeRandomBytes( writer, MAX_SIZE );
        FileChunk readChunkA = fileSender.readChunk( allocator );

        // then
        assertEquals( FileChunk.create( chunkA, true ), readChunkA );
        assertTrue( fileSender.isEndOfInput() );

        // when
        writeRandomBytes( writer, MAX_SIZE );

        // then
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    public void sendLargerFileWhichGrows() throws Exception
    {
        // given
        File file = testDirectory.file( "file" );
        StoreChannel writer = fs.create( file );
        FileSender fileSender = new FileSender( new StoreResource( file, null, 16, pageCache, fs ) );

        // when
        byte[] chunkA = writeRandomBytes( writer, MAX_SIZE );
        byte[] chunkB = writeRandomBytes( writer, MAX_SIZE );
        FileChunk readChunkA = fileSender.readChunk( allocator );

        // then
        assertEquals( FileChunk.create( chunkA, false ), readChunkA );
        assertFalse( fileSender.isEndOfInput() );

        // when
        byte[] chunkC = writeRandomBytes( writer, MAX_SIZE );
        FileChunk readChunkB = fileSender.readChunk( allocator );

        // then
        assertEquals( FileChunk.create( chunkB, false ), readChunkB );
        assertFalse( fileSender.isEndOfInput() );

        // when
        FileChunk readChunkC = fileSender.readChunk( allocator );
        assertEquals( FileChunk.create( chunkC, true ), readChunkC );

        // then
        assertTrue( fileSender.isEndOfInput() );
        assertNull( fileSender.readChunk( allocator ) );
    }

    @Test
    public void sendLargeFileWithUnreliableReadBufferSize() throws Exception
    {
        // given
        byte[] bytes = new byte[MAX_SIZE * 3];
        random.nextBytes( bytes );

        File smallFile = testDirectory.file( "smallFile" );
        try ( StoreChannel storeChannel = fs.create( smallFile ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }

        Adversary adversary = new RandomAdversary( 0.9, 0.0, 0.0 );
        AdversarialFileSystemAbstraction afs = new AdversarialFileSystemAbstraction( adversary, fs );
        FileSender fileSender = new FileSender( new StoreResource( smallFile, null, 16, pageCache, afs ) );

        // when + then
        assertFalse( fileSender.isEndOfInput() );
        assertEquals( FileChunk.create( copyOfRange( bytes, 0, MAX_SIZE ), false ), fileSender.readChunk( allocator ) );
        assertEquals( FileChunk.create( copyOfRange( bytes, MAX_SIZE, MAX_SIZE * 2 ), false ), fileSender.readChunk( allocator ) );
        assertEquals( FileChunk.create( copyOfRange( bytes, MAX_SIZE * 2, bytes.length ), true ), fileSender.readChunk( allocator ) );
        assertNull( fileSender.readChunk( allocator ) );
        assertTrue( fileSender.isEndOfInput() );
    }

    private byte[] writeRandomBytes( StoreChannel writer, int size ) throws IOException
    {
        byte[] bytes = new byte[size];
        random.nextBytes( bytes );
        writer.writeAll( ByteBuffer.wrap( bytes ) );
        return bytes;
    }
}
