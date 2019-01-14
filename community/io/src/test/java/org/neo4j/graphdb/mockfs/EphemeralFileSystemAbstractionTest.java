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
package org.neo4j.graphdb.mockfs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;

import static java.nio.ByteBuffer.allocate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EphemeralFileSystemAbstractionTest
{

    private EphemeralFileSystemAbstraction fs;

    @Before
    public void setUp()
    {
        fs = new EphemeralFileSystemAbstraction();
    }

    @After
    public void tearDown() throws IOException
    {
        fs.close();
    }

    @Test
    public void allowStoreThatExceedDefaultSize() throws IOException
    {
        File aFile = new File( "test" );
        StoreChannel channel = fs.open( aFile, OpenMode.READ_WRITE );

        ByteBuffer buffer = allocate( Long.BYTES );
        int mebiBytes = (int) ByteUnit.mebiBytes( 1 );
        for ( int position = mebiBytes + 42; position < 10_000_000; position += mebiBytes )
        {
            buffer.putLong( 1 );
            buffer.flip();
            channel.writeAll( buffer, position );
            buffer.clear();
        }
        channel.close();
    }

    @Test
    public void growEphemeralFileBuffer()
    {
        EphemeralFileSystemAbstraction.DynamicByteBuffer byteBuffer =
                new EphemeralFileSystemAbstraction.DynamicByteBuffer();

        byte[] testBytes = {1, 2, 3, 4};
        int length = testBytes.length;
        byteBuffer.put( 0, testBytes, 0, length );
        assertEquals( (int) ByteUnit.kibiBytes( 1 ), byteBuffer.buf().capacity() );

        byteBuffer.put( (int) (ByteUnit.kibiBytes( 1 ) + 2), testBytes, 0, length );
        assertEquals( (int) ByteUnit.kibiBytes( 2 ), byteBuffer.buf().capacity() );

        byteBuffer.put( (int) (ByteUnit.kibiBytes( 5 ) + 2), testBytes, 0, length );
        assertEquals( (int) ByteUnit.kibiBytes( 8 ), byteBuffer.buf().capacity() );

        byteBuffer.put( (int) (ByteUnit.mebiBytes( 2 ) + 2), testBytes, 0, length );
        assertEquals( (int) ByteUnit.mebiBytes( 4 ), byteBuffer.buf().capacity() );
    }

    @Test
    public void shouldNotLoseDataForcedBeforeFileSystemCrashes() throws Exception
    {
        try ( EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction() )
        {
            // given
            int numberOfBytesForced = 8;

            File aFile = new File( "yo" );

            StoreChannel channel = fs.open( aFile, OpenMode.READ_WRITE );
            writeLong( channel, 1111 );

            // when
            channel.force( true );
            writeLong( channel, 2222 );
            fs.crash();

            // then
            StoreChannel readChannel = fs.open( aFile, OpenMode.READ );
            assertEquals( numberOfBytesForced, readChannel.size() );

            assertEquals( 1111, readLong( readChannel ).getLong() );
        }
    }

    @Test
    public void shouldBeConsistentAfterConcurrentWritesAndCrashes() throws Exception
    {
        ExecutorService executorService = Executors.newCachedThreadPool();
        try ( EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction() )
        {
            File aFile = new File( "contendedFile" );
            for ( int attempt = 0; attempt < 100; attempt++ )
            {
                Collection<Callable<Void>> workers = new ArrayList<>();
                for ( int i = 0; i < 100; i++ )
                {
                    workers.add( () ->
                    {
                        try
                        {
                            StoreChannel channel = fs.open( aFile, OpenMode.READ_WRITE );
                            channel.position( 0 );
                            writeLong( channel, 1 );
                        }
                        catch ( IOException e )
                        {
                            throw new RuntimeException( e );
                        }
                        return null;
                    } );

                    workers.add( () ->
                    {
                        fs.crash();
                        return null;
                    } );
                }

                List<Future<Void>> futures = executorService.invokeAll( workers );
                for ( Future<Void> future : futures )
                {
                    future.get();
                }
                verifyFileIsEitherEmptyOrContainsLongIntegerValueOne( fs.open( aFile, OpenMode.READ_WRITE ) );
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Test
    public void shouldBeConsistentAfterConcurrentWritesAndForces() throws Exception
    {
        ExecutorService executorService = Executors.newCachedThreadPool();

        try
        {
            for ( int attempt = 0; attempt < 100; attempt++ )
            {
                try ( EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction() )
                {
                    File aFile = new File( "contendedFile" );

                    Collection<Callable<Void>> workers = new ArrayList<>();
                    for ( int i = 0; i < 100; i++ )
                    {
                        workers.add( () ->
                        {
                            try
                            {
                                StoreChannel channel = fs.open( aFile, OpenMode.READ_WRITE );
                                channel.position( channel.size() );
                                writeLong( channel, 1 );
                            }
                            catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                            return null;
                        } );

                        workers.add( () ->
                        {
                            StoreChannel channel = fs.open( aFile, OpenMode.READ_WRITE );
                            channel.force( true );
                            return null;
                        } );
                    }

                    List<Future<Void>> futures = executorService.invokeAll( workers );
                    for ( Future<Void> future : futures )
                    {
                        future.get();
                    }

                    fs.crash();
                    verifyFileIsFullOfLongIntegerOnes( fs.open( aFile, OpenMode.READ_WRITE ) );
                }
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }

    @Test
    public void releaseResourcesOnClose() throws IOException
    {
        try ( EphemeralFileSystemAbstraction fileSystemAbstraction = new EphemeralFileSystemAbstraction() )
        {
            CloseTrackingFileSystem closeTrackingFileSystem = new CloseTrackingFileSystem();
            fileSystemAbstraction.getOrCreateThirdPartyFileSystem( CloseTrackingFileSystem.class,
                    closeTrackingFileSystemClass -> closeTrackingFileSystem );
            File testDir = new File( "testDir" );
            File testFile = new File( "testFile" );
            fileSystemAbstraction.mkdir( testDir );
            fileSystemAbstraction.create( testFile );

            assertTrue( fileSystemAbstraction.fileExists( testFile ) );
            assertTrue( fileSystemAbstraction.fileExists( testFile ) );
            assertFalse( closeTrackingFileSystem.isClosed() );

            fileSystemAbstraction.close();

            assertTrue( closeTrackingFileSystem.isClosed() );
            assertTrue( fileSystemAbstraction.isClosed() );
            assertFalse( fileSystemAbstraction.fileExists( testFile ) );
            assertFalse( fileSystemAbstraction.fileExists( testFile ) );
        }
    }

    private void verifyFileIsFullOfLongIntegerOnes( StoreChannel channel )
    {
        try
        {
            long claimedSize = channel.size();
            ByteBuffer buffer = allocate( (int) claimedSize );
            channel.readAll( buffer );
            buffer.flip();

            for ( int position = 0; position < claimedSize; position += 8 )
            {
                long value = buffer.getLong( position );
                assertEquals( 1, value );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void verifyFileIsEitherEmptyOrContainsLongIntegerValueOne( StoreChannel channel )
    {
        try
        {
            long claimedSize = channel.size();
            ByteBuffer buffer = allocate( 8 );
            channel.read( buffer, 0 );
            buffer.flip();

            if ( claimedSize == 8 )
            {
                assertEquals( 1, buffer.getLong() );
            }
            else
            {
                try
                {
                    buffer.getLong();
                    fail( "Should have thrown an exception" );
                }
                catch ( BufferUnderflowException e )
                {
                    // expected
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private ByteBuffer readLong( StoreChannel readChannel ) throws IOException
    {
        ByteBuffer readBuffer = allocate( 8 );
        readChannel.readAll( readBuffer );
        readBuffer.flip();
        return readBuffer;
    }

    private void writeLong( StoreChannel channel, long value ) throws IOException
    {
        ByteBuffer buffer = allocate( 8 );
        buffer.putLong( value );
        buffer.flip();
        channel.write( buffer );
    }
}
