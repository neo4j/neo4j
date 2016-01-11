/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.graphdb.mockfs;

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

import org.junit.Test;

import org.neo4j.io.fs.StoreChannel;

import static java.nio.ByteBuffer.allocateDirect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EphemeralFileSystemAbstractionCrashTest
{
    @Test
    public void shouldNotLoseDataForcedBeforeFileSystemCrashes() throws Exception
    {
        // given
        int numberOfBytesForced = 8;

        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        File aFile = new File( "yo" );

        StoreChannel channel = fs.open( aFile, "rw" );
        writeLong( channel, 1111 );

        // when
        channel.force( true );
        writeLong( channel, 2222 );
        fs.crash();

        // then
        StoreChannel readChannel = fs.open( aFile, "r" );
        assertEquals( numberOfBytesForced, readChannel.size() );

        assertEquals( 1111, readLong( readChannel ).getLong() );
    }

    @Test
    public void shouldBeConsistentAfterConcurrentWritesAndCrashes() throws Exception
    {
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        File aFile = new File( "contendedFile" );

        ExecutorService executorService = Executors.newCachedThreadPool();

        for ( int attempt = 0; attempt < 100; attempt++ )
        {
            Collection<Callable<Void>> workers = new ArrayList<>();
            for ( int i = 0; i < 100; i++ )
            {
                workers.add( () -> {
                    try
                    {
                        StoreChannel channel = fs.open( aFile, "rw" );
                        channel.position( 0 );
                        writeLong( channel, 1 );
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                    return null;
                } );

                workers.add( () -> {
                    fs.crash();
                    return null;
                } );
            }

            List<Future<Void>> futures = executorService.invokeAll( workers );
            for ( Future<Void> future : futures )
            {
                future.get();
            }
            verifyFileIsEitherEmptyOrContainsLongIntegerValueOne( fs.open( aFile, "rw" ) );
        }

        executorService.shutdown();
    }

    @Test
    public void shouldBeConsistentAfterConcurrentWritesAndForces() throws Exception
    {
        ExecutorService executorService = Executors.newCachedThreadPool();

        for ( int attempt = 0; attempt < 100; attempt++ )
        {
            EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
            File aFile = new File( "contendedFile" );

            Collection<Callable<Void>> workers = new ArrayList<>();
            for ( int i = 0; i < 100; i++ )
            {
                workers.add( () -> {
                    try
                    {
                        StoreChannel channel = fs.open( aFile, "rw" );
                        channel.position( channel.size() );
                        writeLong( channel, 1 );
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                    return null;
                } );

                workers.add( () -> {
                    StoreChannel channel = fs.open( aFile, "rw" );
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
            verifyFileIsFullOfLongIntegerOnes( fs.open( aFile, "rw" ) );
        }

        executorService.shutdown();
    }

    private void verifyFileIsFullOfLongIntegerOnes( StoreChannel channel )
    {
        try
        {
            long claimedSize = channel.size();
            ByteBuffer buffer = allocateDirect( (int) claimedSize );
            channel.read( buffer, 0 );
            buffer.flip();

            for ( int position = 0; position < claimedSize; position+=8 )
            {
                long value = buffer.getLong( position );
                assertEquals(1, value);
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
            ByteBuffer buffer = allocateDirect( 8 );
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
        ByteBuffer readBuffer = allocateDirect( 8 );
        readChannel.read( readBuffer );
        readBuffer.flip();
        return readBuffer;
    }

    private void writeLong( StoreChannel channel, long value ) throws IOException
    {
        ByteBuffer buffer = allocateDirect( 8 );
        buffer.putLong( value );
        buffer.flip();
        channel.write( buffer );
    }
}