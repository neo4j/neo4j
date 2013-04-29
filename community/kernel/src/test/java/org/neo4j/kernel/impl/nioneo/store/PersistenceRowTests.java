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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.test.TargetDirectory;

public class PersistenceRowTests
{
    private static final TargetDirectory target = TargetDirectory.forTest( PersistenceRowTests.class );
    private static final Random RANDOM = new Random();
    private static final int RECORD_SIZE = 7;

    @Rule
    public final TargetDirectory.TestDirectory directory = target.testDirectory();

    @Rule
    public TestName testName = new TestName();

    private PersistenceRow window;

    @Before
    public void before() throws Exception
    {
        String filename = new File( directory.directory(), testName.getMethodName() ).getAbsolutePath();
        RandomAccessFile file = new RandomAccessFile( filename, "rw" );
        FileChannel channel = file.getChannel();
        window = new PersistenceRow( 0, RECORD_SIZE, channel );
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
        // ad infimum, or at least up to RECORD_SIZE

        assertThat( window.getBuffer().getBuffer().position(), is( 4 ) );

        // then a flush comes along...
        window.force();

        assertThat( window.getBuffer().getBuffer().position(), is( 4 ) );
    }
}
