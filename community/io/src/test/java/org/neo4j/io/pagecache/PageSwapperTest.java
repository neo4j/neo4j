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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.impl.ByteBufferPage;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class PageSwapperTest
{
    private static PageEvictionCallback NO_CALLBACK = new PageEvictionCallback()
    {
        @Override
        public void onEvict( long pageId, Page page )
        {
        }
    };

    protected static EphemeralFileSystemAbstraction fs;
    protected static final int cachePageSize = 32;

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

    protected abstract PageSwapperFactory swapperFactory( FileSystemAbstraction fs );

    private ByteBufferPage createPage( int cachePageSize )
    {
        return new ByteBufferPage( ByteBuffer.allocateDirect( cachePageSize ) );
    }

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

        ByteBufferPage page = createPage( cachePageSize );
        PageSwapperFactory swapperFactory = swapperFactory( fs );
        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );

        Thread.currentThread().interrupt();

        swapper.write( 0, page );
        assertTrue( Thread.currentThread().isInterrupted() );
    }

    @Test
    public void forcingMustNotSwallowInterrupts() throws IOException
    {
        File file = new File( "a" );
        fs.create( file ).close();

        PageSwapperFactory swapperFactory = swapperFactory( fs );
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

        ByteBufferPage page = createPage( cachePageSize );
        File file = new File( "a" );
        StoreChannel channel = fs.create( file );
        ByteBuffer buf = ByteBuffer.allocate( cachePageSize );
        buf.putLong( x );
        buf.putLong( y );
        buf.putInt( z );
        buf.flip();
        channel.writeAll( buf );
        channel.close();

        PageSwapperFactory swapperFactory = swapperFactory( fs );
        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );

        Thread.currentThread().interrupt();

        swapper.read( 0, page );

        // Clear the interrupted flag and assert that it was still raised
        assertTrue( Thread.interrupted() );

        assertThat( page.getLong( 0 ), is( x ) );
        assertThat( page.getLong( 8 ), is( y ) );
        assertThat( page.getInt( 16 ), is( z ) );

        // This must not throw because we should still have a usable channel
        swapper.force();
    }

    @Test
    public void mustReopenChannelWhenWriteFailsWithAsynchronousCloseException() throws IOException
    {
        long x = ThreadLocalRandom.current().nextLong();
        long y = ThreadLocalRandom.current().nextLong();
        int z = ThreadLocalRandom.current().nextInt();

        ByteBufferPage page = createPage( cachePageSize );
        page.putLong( x, 0 );
        page.putLong( y, 8 );
        page.putInt( z, 16 );
        File file = new File( "a" );
        fs.create( file ).close();

        PageSwapperFactory swapperFactory = swapperFactory( fs );
        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );

        Thread.currentThread().interrupt();

        swapper.write( 0, page );

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

        PageSwapperFactory swapperFactory = swapperFactory( fs );
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

        ByteBufferPage page = createPage( cachePageSize );
        PageSwapperFactory swapperFactory = swapperFactory( fs );
        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );
        swapper.close();

        try
        {
            swapper.read( 0, page );
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
    }

    @Test
    public void writeMustNotReopenExplicitlyClosedChannel() throws IOException
    {
        File file = new File( "a" );
        fs.create( file ).close();

        ByteBufferPage page = createPage( cachePageSize );
        PageSwapperFactory swapperFactory = swapperFactory( fs );
        PageSwapper swapper = swapperFactory.createPageSwapper( file, cachePageSize, NO_CALLBACK );
        swapper.close();

        try
        {
            swapper.write( 0, page );
            fail( "Should have thrown because the channel should be closed" );
        }
        catch ( ClosedChannelException ignore )
        {
            // This is fine.
        }
    }

    @Test
    public void forceMustNotReopenExplicitlyClosedChannel() throws IOException
    {
        File file = new File( "a" );
        fs.create( file ).close();

        PageSwapperFactory swapperFactory = swapperFactory( fs );
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
