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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageSwapper;

import static org.junit.Assert.assertThat;

import static org.neo4j.test.ByteArrayMatcher.byteArray;

public class SingleFilePageSwapperTest
{
    private final File file = new File( "file" );

    private EphemeralFileSystemAbstraction fs;
    private SingleFilePageSwapperFactory factory;

    @Before
    public void setUp()
    {
        fs = new EphemeralFileSystemAbstraction();
        factory = new SingleFilePageSwapperFactory( fs );
    }

    @After
    public void tearDown()
    {
        fs.shutdown();
    }

    @Test
    public void swappingInMustFillPageWithData() throws IOException
    {
        byte[] bytes = new byte[] { 1, 2, 3, 4 };
        StoreChannel channel = fs.create( file );
        channel.writeAll( ByteBuffer.wrap( bytes ) );
        channel.close();

        PageSwapper swapper = factory.createPageSwapper( file, 4, null );
        ByteBuffer target = ByteBuffer.allocate( 4 );
        ByteBufferPage page = new ByteBufferPage( target );
        swapper.read( 0, page );

        assertThat( target.array(), byteArray( bytes ) );
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
        channel.writeAll( ByteBuffer.wrap( bytes ) );
        channel.close();

        PageSwapper swapper = factory.createPageSwapper( file, 4, null );
        ByteBuffer target = ByteBuffer.allocate( 4 );
        ByteBufferPage page = new ByteBufferPage( target );
        swapper.read( 1, page );

        assertThat( target.array(), byteArray( new byte[]{ 5, 6, 0, 0 } ) );
    }

    @Test
    public void swappingOutMustWritePageToFile() throws IOException
    {
        fs.create( file ).close();

        byte[] expected = new byte[] { 1, 2, 3, 4 };
        ByteBufferPage page = new ByteBufferPage( ByteBuffer.wrap( expected ) );

        PageSwapper swapper = factory.createPageSwapper( file, 4, null );
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
        channel.writeAll( ByteBuffer.wrap( initialData ) );
        channel.close();

        byte[] change = new byte[] { 8, 7, 6, 5 };
        ByteBufferPage page = new ByteBufferPage( ByteBuffer.wrap( change ) );

        PageSwapper swapper = factory.createPageSwapper( file, 4, null );
        swapper.write( 1, page );

        InputStream stream = fs.openAsInputStream( file );
        byte[] actual = new byte[(int) fs.getFileSize( file )];
        stream.read( actual );

        assertThat( actual, byteArray( finalData ) );
    }
}
