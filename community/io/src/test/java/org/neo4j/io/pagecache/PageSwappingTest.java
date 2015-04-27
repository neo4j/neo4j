/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.io.pagecache;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public abstract class PageSwappingTest
{
    private static PageEvictionCallback NO_CALLBACK = new PageEvictionCallback()
    {
        @Override
        public void onEvict( long pageId, Page page )
        {
        }
    };

    private static EphemeralFileSystemAbstraction fs;

    private final PageSwapperFactory swapperFactory;
    private final int cachePageSize = 32;

    @BeforeClass
    public static void setUp()
    {
        Thread.interrupted(); // Clear stray interrupts
        fs = new EphemeralFileSystemAbstraction();
    }

    @AfterClass
    public static void tearDown()
    {
        fs.shutdown();
    }

    @Parameterized.Parameters
    public static Iterable<Object[]> dataPoints()
    {
        Factory<PageSwapperFactory> singleFileSwapper = new Factory<PageSwapperFactory>()
        {
            @Override
            public PageSwapperFactory newInstance()
            {
                return new SingleFilePageSwapperFactory( fs );
            }
        };

        return Arrays.asList( new Object[][]{
                { singleFileSwapper }
                } );
    }

    public PageSwappingTest( Factory<PageSwapperFactory> fixture )
    {
        swapperFactory = fixture.newInstance();
    }

    protected abstract Page createPage( int cachePageSize );

    protected abstract long writeLock( Page page );

    protected abstract void unlockWrite( Page page, long stamp );

    protected abstract long getLong( Page page, int offset );

    protected abstract int getInt( Page page, int offset );

    protected abstract void putLong( Page page, long value, int offset );

    protected abstract void putInt( Page page, int value, int offset );

    @Before
    @After
    public void clearStrayInterrupts()
    {
        Thread.interrupted();
    }

    @Test
    public void swappingOutMustNotSwallowInterrupts() throws IOException
    {
        File file = new File( "a" );
        fs.create( file ).close();

        Page page = createPage( cachePageSize );
        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );

        Thread.currentThread().interrupt();

        long stamp = writeLock( page );
        try
        {
            swapper.write( 0, page );
            assertTrue( Thread.currentThread().isInterrupted() );
        }
        finally
        {
            unlockWrite( page, stamp );
        }
    }

    @Test
    public void forcingMustNotSwallowInterrupts() throws IOException
    {
        File file = new File( "a" );
        fs.create( file ).close();

        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );

        Thread.currentThread().interrupt();
        swapper.force();
        assertTrue( Thread.currentThread().isInterrupted() );
    }

    @Test
    public void mustReopenChannelWhenReadFailsWithAsynchronousCloseException() throws IOException
    {
        long x = ThreadLocalRandom.current().nextLong();
        long y = ThreadLocalRandom.current().nextLong();
        int z = ThreadLocalRandom.current().nextInt();

        Page page = createPage( cachePageSize );
        File file = new File( "a" );
        StoreChannel channel = fs.create( file );
        ByteBuffer buf = ByteBuffer.allocate( cachePageSize );
        buf.putLong( x );
        buf.putLong( y );
        buf.putInt( z );
        buf.flip();
        channel.writeAll( buf );
        channel.close();

        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );

        Thread.currentThread().interrupt();

        long stamp = writeLock( page );
        try
        {
            swapper.read( 0, page );
        }
        finally
        {
            unlockWrite( page, stamp );
        }

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        assertThat( getLong( page, 0 ), is( x ) );
        assertThat( getLong( page, 8 ), is( y ) );
        assertThat( getInt( page, 16 ), is( z ) );

        // This must not throw because we should still have a usable channel
        swapper.force();
    }

    @Test
    public void mustReopenChannelWhenWriteFailsWithAsynchronousCloseException() throws IOException
    {
        long x = ThreadLocalRandom.current().nextLong();
        long y = ThreadLocalRandom.current().nextLong();
        int z = ThreadLocalRandom.current().nextInt();

        Page page = createPage( cachePageSize );
        putLong( page, x, 0 );
        putLong( page, y, 8 );
        putInt( page, z, 16 );
        File file = new File( "a" );
        fs.create( file ).close();

        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );

        Thread.currentThread().interrupt();

        long stamp = writeLock( page );
        try
        {
            swapper.write( 0, page );
        }
        finally
        {
            unlockWrite( page, stamp );
        }

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        // This must not throw because we should still have a usable channel
        swapper.force();

        ByteBuffer buf = ByteBuffer.allocate( cachePageSize );
        StoreChannel channel = fs.open( file, "r" );
        int bytesRead = channel.read( buf );
        channel.close();
        assertThat( bytesRead, is( cachePageSize ) );
        buf.flip();
        assertThat( buf.getLong(), is( x ) );
        assertThat( buf.getLong(), is( y ) );
        assertThat( buf.getInt(), is( z ) );
    }

    @Test
    public void mustReopenChannelWhenForceFailsWithAsynchronousCloseException() throws IOException
    {
        File file = new File( "a" );
        fs.create( file ).close();

        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );

        for ( int i = 0; i < 10; i++ )
        {
            Thread.currentThread().interrupt();

            // This must not throw
            swapper.force();

            // Clear the interrupted flag and assert that it was still raised
            assertTrue( Thread.interrupted() );
        }
    }

    @Test
    public void readMustNotReopenExplicitlyClosedChannel() throws IOException
    {
        File file = new File( "a" );
        StoreChannel channel = fs.create( file );
        ByteBuffer buf = ByteBuffer.allocate( cachePageSize );
        channel.writeAll( buf );
        channel.close();

        Page page = createPage( cachePageSize );
        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );
        swapper.close();

        long stamp = writeLock( page );
        try
        {
            swapper.read( 0, page );
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
        finally
        {
            unlockWrite( page, stamp );
        }
    }

    @Test
    public void writeMustNotReopenExplicitlyClosedChannel() throws IOException
    {
        File file = new File( "a" );
        fs.create( file ).close();

        Page page = createPage( cachePageSize );
        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );
        swapper.close();

        long stamp = writeLock( page );
        try
        {
            swapper.write( 0, page );
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
        finally
        {
            unlockWrite( page, stamp );
        }
    }

    @Test
    public void forceMustNotReopenExplicitlyClosedChannel() throws IOException
    {
        File file = new File( "a" );
        fs.create( file ).close();

        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );
        swapper.close();

        try
        {
            swapper.force();
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
    }
}
