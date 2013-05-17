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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.kernel.impl.util.StringLogger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

/**
 * Tests or rather measures contention imposed by
 * {@link PersistenceWindowPool#acquire(long, OperationType) acquiring persistence window pool windows}.
 * This test should be moved to a performance benchmark suite or similar,
 * but exists here because it's getting developed along side changes to
 * {@link PersistenceWindowPool} in kernel.
 * 
 * @author Mattias Persson
 */
@Ignore( "Not a proper test really, merely a contention measurement" )
public class TestPersistenceWindowPoolContention
{
    private static final int recordSize = 30;
    private static final long mappingSize = giga( 1 );
    
    private long fileSize = mega( 800 );
    private FileChannel channel;
    private PersistenceWindowPool pool;
    private final Map<Long, Long> values = new ConcurrentHashMap<Long, Long>();
    
    @Before
    public void before() throws Exception
    {
        File file = new File( "target/bigfile" );
        assertTrue( "delete " + file, file.delete() );
        channel = new RandomAccessFile( file, "rw" ).getChannel();
        write( channel, fileSize );
        pool = new PersistenceWindowPool( new File("contention test"), recordSize, channel, mappingSize, true, false, StringLogger.DEV_NULL );
    }

    private void write( FileChannel channel, long bytes ) throws IOException
    {
        channel.position( bytes );
        channel.write( ByteBuffer.wrap( new byte[1] ) );
        channel.position( 0 );
        channel.force( true );
    }

    @After
    public void after() throws Exception
    {
        // close() is package-access, so a little good ol' reflection
        Method closeMethod = pool.getClass().getDeclaredMethod( "close" );
        closeMethod.setAccessible( true );
        closeMethod.invoke( pool );
        channel.close();
    }
    
    private static long kilo( long i )
    {
        return i*1024;
    }
    
    private static long mega( long i )
    {
        return kilo( kilo( i ) );
    }
    
    private static long giga( long i )
    {
        return mega( kilo( i ) );
    }
    
    @Test
    public void triggerContentionAmongstPersistenceWindows() throws Exception
    {
        List<Worker> workers = new ArrayList<Worker>();
        for ( int i = 0; i < 8; i++ )
        {
            Worker worker = new Worker();
            workers.add( worker );
            worker.start();
        }

        long endTime = System.currentTimeMillis() + SECONDS.toMillis( 60*3 );
        int tick = 2;
        while ( System.currentTimeMillis() < endTime )
        {
            Thread.sleep( SECONDS.toMillis( tick ) );
            System.out.println( getPoolStats() );
            fileSize += mega( tick );
        }
        
        for ( Worker worker : workers )
            worker.halted = true;
        long total = 0;
        for ( Worker putter : workers )
            total += putter.waitForEnd();
        
        System.out.println( "total:" + total );
    }
    
    private String getPoolStats() throws Exception
    {
        Method method = pool.getClass().getDeclaredMethod( "getStats" );
        method.setAccessible( true );
        return method.invoke( pool ).toString();
    }

    private class Worker extends Thread
    {
        private volatile boolean halted;
        private final Random random = new Random();
        private long count;
        
        @Override
        public void run()
        {
            warmItUp();
            while ( !halted )
            {
                OperationType type = randomOperationTypeButFavoringReads(0.6f);
                long id = randomPosition();
                PersistenceWindow window = pool.acquire( id, type );
                try
                {
                    switch ( type )
                    {
                    case READ:
                        readStuff( window, id );
                        break;
                    case WRITE:
                        writeStuff( window, id );
                        break;
                    }
                }
                finally
                {
                    pool.release( window );
                }
                count++;
            }
        }
        
        private void warmItUp()
        {
            for ( int i = 0; i < 100000; i++ )
            {
                long id = randomPosition();
                PersistenceWindow window = pool.acquire( id, OperationType.READ );
                try
                {
                    readStuff( window, id );
                }
                finally
                {
                    pool.release( window );
                }
            }
        }

        private synchronized long waitForEnd() throws InterruptedException
        {
            join();
            return count;
        }

        private void readStuff( PersistenceWindow window, long id )
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            long read = buffer.getLong();
            
            // Just having this Map lookup affects the test too much.
            Long existingValue = values.get( id );
            if ( existingValue != null )
                Assert.assertEquals( existingValue.longValue(), read );
        }
        
        private void writeStuff( PersistenceWindow window, long id )
        {
            Buffer buffer = window.getOffsettedBuffer( id );
            long value = random.nextLong();
            buffer.putLong( value );
            
            // Just having this Map lookup affects the test too much.
            values.put( id, value );
        }

        private long randomPosition()
        {
            return random.nextInt( (int)(fileSize/recordSize) );
        }

        private OperationType randomOperationTypeButFavoringReads( float percentageReads )
        {
            return random.nextFloat() <= percentageReads ? OperationType.READ : OperationType.WRITE;
        }
    }
}
