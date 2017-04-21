/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.io.fs;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import org.neo4j.graphdb.mockfs.CloseTrackingFileSystem;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class FileSystemAbstractionTest
{
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory( getClass() );

    protected FileSystemAbstraction fsa;
    protected File path;

    @Before
    public void before() throws Exception
    {
        fsa = buildFileSystemAbstraction();
        path = new File( dir.directory(), UUID.randomUUID().toString() );
    }

    @After
    public void tearDown() throws Exception
    {
        fsa.close();
    }

    protected abstract FileSystemAbstraction buildFileSystemAbstraction();

    @Test
    public void shouldCreatePath() throws Exception
    {
        fsa.mkdirs( path );

        assertTrue( fsa.fileExists( path ) );
    }

    @Test
    public void shouldCreateDeepPath() throws Exception
    {
        path = new File( path, UUID.randomUUID() + "/" + UUID.randomUUID() );

        fsa.mkdirs( path );

        assertTrue( fsa.fileExists( path ) );
    }

    @Test
    public void shouldCreatePathThatAlreadyExists() throws Exception
    {
        assertTrue( fsa.mkdir( path ) );

        fsa.mkdirs( path );

        assertTrue( fsa.fileExists( path ) );
    }

    @Test
    public void shouldCreatePathThatPointsToFile() throws Exception
    {
        assertTrue( fsa.mkdir( path ) );
        path = new File( path, "some_file" );
        try ( StoreChannel channel = fsa.create( path ) )
        {
            assertThat( channel, is( not( nullValue() )) );

            fsa.mkdirs( path );

            assertTrue( fsa.fileExists( path ) );
        }
    }

    @Test
    public void moveToDirectoryMustMoveFile() throws Exception
    {
        File source = new File( path, "source" );
        File target = new File( path, "target" );
        File file = new File( source, "file" );
        File fileAfterMove = new File( target, "file" );
        fsa.mkdirs( source );
        fsa.mkdirs( target );
        fsa.create( file ).close();
        assertTrue( fsa.fileExists( file ) );
        assertFalse( fsa.fileExists( fileAfterMove ) );
        fsa.moveToDirectory( file, target );
        assertFalse( fsa.fileExists( file ) );
        assertTrue( fsa.fileExists( fileAfterMove ) );
    }

    @Test
    public void moveToDirectoryMustRecursivelyMoveAllFilesInGivenDirectory() throws Exception
    {
        File source = new File( path, "source" );
        File target = new File( path, "target" );
        File file = new File( source, "file" );
        File sourceAfterMove = new File( target, "source" );
        File fileAfterMove = new File( sourceAfterMove, "file" );
        fsa.mkdirs( source );
        fsa.mkdirs( target );
        fsa.create( file ).close();
        assertTrue( fsa.fileExists( source ) );
        assertTrue( fsa.fileExists( file ) );
        assertFalse( fsa.fileExists( sourceAfterMove ) );
        assertFalse( fsa.fileExists( fileAfterMove ) );
        fsa.moveToDirectory( source, target );
        assertFalse( fsa.fileExists( source ) );
        assertFalse( fsa.fileExists( file ) );
        assertTrue( fsa.fileExists( sourceAfterMove ) );
        assertTrue( fsa.fileExists( fileAfterMove ) );
    }

    @Test
    public void deleteRecursivelyMustDeleteAllFilesInDirectory() throws Exception
    {
        fsa.mkdirs( path );
        File a = new File( path, "a" );
        fsa.create( a ).close();
        File b = new File( path, "b" );
        fsa.create( b ).close();
        File c = new File( path, "c" );
        fsa.create( c ).close();
        File d = new File( path, "d" );
        fsa.create( d ).close();

        fsa.deleteRecursively( path );

        assertFalse( fsa.fileExists( a ) );
        assertFalse( fsa.fileExists( b ) );
        assertFalse( fsa.fileExists( c ) );
        assertFalse( fsa.fileExists( d ) );
    }

    @Test
    public void deleteRecursivelyMustDeleteGivenDirectory() throws Exception
    {
        fsa.mkdirs( path );
        fsa.deleteRecursively( path );
        assertFalse( fsa.fileExists( path ) );
    }

    @Test
    public void deleteRecursivelyMustDeleteGivenFile() throws Exception
    {
        fsa.mkdirs( path );
        File file = new File( path, "file" );
        fsa.create( file ).close();
        fsa.deleteRecursively( file );
        assertFalse( fsa.fileExists( file ) );
    }

    @Test
    public void fileWatcherCreation() throws IOException
    {
        try ( FileWatcher fileWatcher = fsa.fileWatcher() )
        {
            assertNotNull( fileWatcher.watch( dir.directory( "testDirectory" ) ) );
        }
    }

    @Test
    public void closeThirdPartyFileSystemsOnClose() throws IOException
    {
        CloseTrackingFileSystem closeTrackingFileSystem = new CloseTrackingFileSystem();

        CloseTrackingFileSystem fileSystem = fsa.getOrCreateThirdPartyFileSystem( CloseTrackingFileSystem.class,
                thirdPartyFileSystemClass -> closeTrackingFileSystem );

        assertSame( closeTrackingFileSystem, fileSystem );
        assertFalse( closeTrackingFileSystem.isClosed() );

        fsa.close();

        assertTrue( closeTrackingFileSystem.isClosed() );
    }

    @Test
    public void readAndWriteMustTakeBufferPositionIntoAccount() throws Exception
    {
        byte[] bytes = new byte[] {1, 2, 3, 4, 5};
        ByteBuffer buf = ByteBuffer.wrap( bytes );
        buf.position( 1 );

        fsa.mkdirs( path );
        File file = new File( path, "file" );
        try ( StoreChannel channel = fsa.open( file, "rw" ) )
        {
            assertThat( channel.write( buf ), is( 4 ) );
        }
        try ( InputStream stream = fsa.openAsInputStream( file ) )
        {
            assertThat( stream.read(), is( 2 ) );
            assertThat( stream.read(), is( 3 ) );
            assertThat( stream.read(), is( 4 ) );
            assertThat( stream.read(), is( 5 ) );
            assertThat( stream.read(), is( -1 ) );
        }
        Arrays.fill( bytes, (byte) 0 );
        buf.position( 1 );
        try ( StoreChannel channel = fsa.open( file, "rw" ) )
        {
            assertThat( channel.read( buf ), is( 4 ) );
            buf.clear();
            assertThat( buf.get(), is( (byte) 0 ) );
            assertThat( buf.get(), is( (byte) 2 ) );
            assertThat( buf.get(), is( (byte) 3 ) );
            assertThat( buf.get(), is( (byte) 4 ) );
            assertThat( buf.get(), is( (byte) 5 ) );
        }
    }
}
