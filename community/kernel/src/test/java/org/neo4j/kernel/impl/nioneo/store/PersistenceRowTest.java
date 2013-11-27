/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

public class PersistenceRowTest
{
    private static final Random RANDOM = new Random();
    private static final int RECORD_SIZE = 7;

    private PersistenceRow window;
    private FileChannel realChannel;

    @Before
    public void before() throws Exception
    {
        File directory = new File( "target/test-data" );
        directory.mkdirs();
        String filename = new File( directory, UUID.randomUUID().toString() ).getAbsolutePath();
        RandomAccessFile file = new RandomAccessFile( filename, "rw" );
        realChannel = file.getChannel();
        window = new PersistenceRow( 0, RECORD_SIZE, realChannel );
        window.lock( OperationType.WRITE );
    }

    @Test
    public void shouldNotLetChangesToOffsetInterfereWithFlushing() throws Exception
    {
        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                while ( true )
                {
                    // modify buffer's position "because we can" - this is used in several places,
                    // including Buffer.getOffsettedBuffer which in turn is also used in several places
                    window.getBuffer().setOffset( RANDOM.nextInt( window.getBuffer().getBuffer().limit() ) );
                }
            }
        } ).start();

        try
        {
            for ( int i = 1; i < 10000; i++ )
            {
                window.force();
            }
        }
        catch ( BufferOverflowException e )
        {
            fail( "Changing the state of the buffer's flags should not affect flushing" );
        }
    }

    @Test
    public void shouldNotLetFlushingInterfereWithReads() throws Exception
    {
        window.getBuffer().get();
        window.getBuffer().get();
        window.getBuffer().get();
        window.getBuffer().get();
        // ad infinitum, or at least up to RECORD_SIZE

        assertThat( window.getBuffer().getBuffer().position(), is( 4 ) );

        // then a flush comes along...
        window.force();

        assertThat( window.getBuffer().getBuffer().position(), is( 4 ) );
    }

    @Test
    public void grabbingWriteLockShouldMarkRowAsDirty() throws Exception
    {
        // GIVEN a channel and a row over it
        FileChannel channel = spy( realChannel );
        PersistenceRow row = new PersistenceRow( 0, 1, channel );

        // WHEN you grab a write lock
        row.lock( OperationType.WRITE );

        // THEN it should be dirty
        assertThat( "Dirty before force", row.isDirty(), is( true ) );
    }

    @Test
    public void forcingARowShouldMarkItAsClean() throws Exception
    {
        // GIVEN a channel and a row over it
        FileChannel channel = spy( realChannel );
        PersistenceRow row = new PersistenceRow( 0, 1, channel );

        // WHEN you grab a write lock and force
        row.lock( OperationType.WRITE );
        row.force();

        // THEN the row should be marked clean and a call made to write to the buffer
        assertThat( "Dirty after force", row.isDirty(), is( false ) );
        verify( channel, times( 1 ) ).write( any( ByteBuffer.class), anyLong() );

        // WHEN you subsequently force again
        row.force();

        // THEN no writes should go through and the status should remain clean
        verify( channel, times( 1 ) ).write( any( ByteBuffer.class), anyLong() );
        assertThat( "Dirty after non-flushing force", row.isDirty(), is( false ) );
    }

    @Test
    public void explicitlyMarkingAsCleanShouldDoSo() throws Exception
    {
        // GIVEN a channel and a row over it
        FileChannel channel = spy( realChannel );
        PersistenceRow row = new PersistenceRow( 0, 1, channel );

        // WHEN you grab a write lock, marking as dirty
        row.lock( OperationType.WRITE );
        // and then manually mark is as clean
        row.setClean();

        // THEN it should be non-dirty
        assertThat( "Dirty after setting clean", row.isDirty(), is( false ) );

    }
}
