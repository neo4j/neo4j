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
package org.neo4j.io.pagecache.impl;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PageSwapperTest;

import static org.junit.Assert.assertThat;
import static org.neo4j.test.ByteArrayMatcher.byteArray;

public class SingleFilePageSwapperTest extends PageSwapperTest
{
    private final File file = new File( "file" );
    private EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();

    @After
    public void tearDown()
    {
        fs.shutdown();
    }

    protected PageSwapperFactory swapperFactory()
    {
        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory();
        factory.setFileSystemAbstraction( fs );
        return factory;
    }

    @Test
    public void swappingInMustFillPageWithData() throws IOException
    {
        byte[] bytes = new byte[] { 1, 2, 3, 4 };
        StoreChannel channel = fs.create( file );
        channel.writeAll( wrap( bytes ) );
        channel.close();

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = factory.createPageSwapper( file, 4, null, false );
        ByteBuffer target = ByteBuffer.allocateDirect( 4 );
        ByteBufferPage page = new ByteBufferPage( target );
        swapper.read( 0, page );

        assertThat( array( target ), byteArray( bytes ) );
    }

    @Test
    public void mustZeroFillPageBeyondEndOfFile() throws IOException
    {
        byte[] bytes = new byte[] {
                // --- page 0:
                1, 2, 3, 4,
                // --- page 1:
                5, 6
        };
        StoreChannel channel = fs.create( file );
        channel.writeAll( wrap( bytes ) );
        channel.close();

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = factory.createPageSwapper( file, 4, null, false );
        ByteBuffer target = ByteBuffer.allocateDirect( 4 );
        ByteBufferPage page = new ByteBufferPage( target );
        swapper.read( 1, page );

        assertThat( array( target ), byteArray( new byte[]{ 5, 6, 0, 0 } ) );
    }

    @Test
    public void swappingOutMustWritePageToFile() throws IOException
    {
        fs.create( file ).close();

        byte[] expected = new byte[] { 1, 2, 3, 4 };
        ByteBufferPage page = new ByteBufferPage( wrap( expected ) );

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = factory.createPageSwapper( file, 4, null, false );
        swapper.write( 0, page );

        InputStream stream = fs.openAsInputStream( file );
        byte[] actual = new byte[expected.length];
        stream.read( actual );

        assertThat( actual, byteArray( expected ) );
    }

    @Test
    public void swappingOutMustNotOverwriteDataBeyondPage() throws IOException
    {
        byte[] initialData = new byte[] {
                // --- page 0:
                1, 2, 3, 4,
                // --- page 1:
                5, 6, 7, 8,
                // --- page 2:
                9, 10
        };
        byte[] finalData = new byte[] {
                // --- page 0:
                1, 2, 3, 4,
                // --- page 1:
                8, 7, 6, 5,
                // --- page 2:
                9, 10
        };
        StoreChannel channel = fs.create( file );
        channel.writeAll( wrap( initialData ) );
        channel.close();

        byte[] change = new byte[] { 8, 7, 6, 5 };
        ByteBufferPage page = new ByteBufferPage( wrap( change ) );

        PageSwapperFactory factory = swapperFactory();
        PageSwapper swapper = factory.createPageSwapper( file, 4, null, false );
        swapper.write( 1, page );

        InputStream stream = fs.openAsInputStream( file );
        byte[] actual = new byte[(int) fs.getFileSize( file )];
        stream.read( actual );

        assertThat( actual, byteArray( finalData ) );
    }

    private byte[] array( ByteBuffer target )
    {
        target.clear();
        byte[] array = new byte[target.capacity()];
        while ( target.position() < target.capacity() )
        {
            array[target.position()] = target.get();
        }
        return array;
    }

    private ByteBuffer wrap( byte[] bytes )
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect( bytes.length );
        for ( byte b : bytes )
        {
            buffer.put( b );
        }
        buffer.clear();
        return buffer;
    }
}
