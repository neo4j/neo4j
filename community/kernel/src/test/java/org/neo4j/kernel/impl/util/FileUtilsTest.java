/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.io.fs.FileUtils.pathToFileAfterMove;
import static org.neo4j.io.fs.FileUtils.size;

public class FileUtilsTest
{
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    public ExpectedException expected = ExpectedException.none();
    public FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Rule
    public RuleChain chain = RuleChain.outerRule( testDirectory ).around( expected );

    private File path;

    @Before
    public void doBefore() throws Exception
    {
        path = testDirectory.directory( "path" );
    }

    @Test
    public void moveFileToDirectory() throws Exception
    {
        File file = touchFile( "source" );
        File targetDir = directory( "dir" );

        File newLocationOfFile = FileUtils.moveFileToDirectory( file, targetDir );
        assertTrue( newLocationOfFile.exists() );
        assertFalse( file.exists() );
        assertEquals( newLocationOfFile, targetDir.listFiles()[0] );
    }

    @Test
    public void moveFile() throws Exception
    {
        File file = touchFile( "source" );
        File targetDir = directory( "dir" );

        File newLocationOfFile = new File( targetDir, "new-name" );
        FileUtils.moveFile( file, newLocationOfFile );
        assertTrue( newLocationOfFile.exists() );
        assertFalse( file.exists() );
        assertEquals( newLocationOfFile, targetDir.listFiles()[0] );
    }

    @Test
    public void testEmptyDirectory() throws IOException
    {
        File emptyDir = directory( "emptyDir" );

        File nonEmptyDir = directory( "nonEmptyDir" );
        File directoryContent = new File( nonEmptyDir, "somefile" );
        assert directoryContent.createNewFile();

        assertTrue( FileUtils.isEmptyDirectory( emptyDir ) );
        assertFalse( FileUtils.isEmptyDirectory( nonEmptyDir ) );
    }

    @Test
    public void pathToFileAfterMoveMustThrowIfFileNotSubPathToFromShorter() throws Exception
    {
        File file = new File( "/a" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        expected.expect( IllegalArgumentException.class );
        pathToFileAfterMove( from, to, file );
    }

    // INVALID
    @Test
    public void pathToFileAfterMoveMustThrowIfFileNotSubPathToFromSameLength() throws Exception
    {
        File file = new File( "/a/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        expected.expect( IllegalArgumentException.class );
        pathToFileAfterMove( from, to, file );
    }

    @Test
    public void pathToFileAfterMoveMustThrowIfFileNotSubPathToFromLonger() throws Exception
    {
        File file = new File( "/a/c/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        expected.expect( IllegalArgumentException.class );
        pathToFileAfterMove( from, to, file );
    }

    @Test
    public void pathToFileAfterMoveMustThrowIfFromDirIsCompletePathToFile() throws Exception
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b/f" );
        File to   = new File( "/a/c" );

        expected.expect( IllegalArgumentException.class );
        pathToFileAfterMove( from, to, file );
    }

    // SIBLING
    @Test
    public void pathToFileAfterMoveMustWorkIfMovingToSibling() throws Exception
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/c/f" ) ) );
    }

    @Test
    public void pathToFileAfterMoveMustWorkIfMovingToSiblingAndFileHasSubDir() throws Exception
    {
        File file = new File( "/a/b/d/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/c/d/f" ) ) );
    }

    // DEEPER
    @Test
    public void pathToFileAfterMoveMustWorkIfMovingToSubDir() throws Exception
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/b/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/b/c/f" ) ) );
    }

    @Test
    public void pathToFileAfterMoveMustWorkIfMovingToSubDirAndFileHasSubDir() throws Exception
    {
        File file = new File( "/a/b/d/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/b/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/b/c/d/f" ) ) );
    }

    @Test
    public void pathToFileAfterMoveMustWorkIfMovingOutOfDir() throws Exception
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/c/f" ) ) );
    }

    @Test
    public void pathToFileAfterMoveMustWorkIfMovingOutOfDirAndFileHasSubDir() throws Exception
    {
        File file = new File( "/a/b/d/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/c/d/f" ) ) );
    }

    @Test
    public void pathToFileAfterMoveMustWorkIfNotMovingAtAll() throws Exception
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/b" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/b/f" ) ) );
    }

    @Test
    public void pathToFileAfterMoveMustWorkIfNotMovingAtAllAndFileHasSubDir() throws Exception
    {
        File file = new File( "/a/b/d/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/b" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/b/d/f" ) ) );
    }

    @Test
    public void allMacsHaveHighIO() throws Exception
    {
        assumeTrue( SystemUtils.IS_OS_MAC );
        assertTrue( FileUtils.highIODevice( Paths.get( "." ), false ) );
    }

    @Test
    public void windowsNeverHaveHighIO() throws Exception
    {
        // Future work: Maybe we should do like on Mac and assume true on Windows as well?
        assumeTrue( SystemUtils.IS_OS_WINDOWS );
        assertFalse( FileUtils.highIODevice( Paths.get( "." ), false ) );
    }

    @Test
    public void onLinuxDevShmHasHighIO() throws Exception
    {
        assumeTrue( SystemUtils.IS_OS_LINUX );
        assertTrue( FileUtils.highIODevice( Paths.get( "/dev/shm" ), false ) );
    }

    @Test
    public void sizeOfFile() throws Exception
    {
        File file = touchFile( "a" );

        try ( FileWriter fileWriter = new FileWriter( file ) )
        {
            fileWriter.append( 'a' );
        }

        assertThat( size( fs, file ), is( 1L )  );
    }

    @Test
    public void sizeOfDirector() throws Exception
    {
        File dir = directory( "dir" );
        File file1 = new File( dir, "file1" );
        File file2 = new File( dir, "file2" );

        try ( FileWriter fileWriter = new FileWriter( file1 ) )
        {
            fileWriter.append( 'a' ).append( 'b' );
        }
        try ( FileWriter fileWriter = new FileWriter( file2 ) )
        {
            fileWriter.append( 'a' );
        }

        assertThat( size( fs, dir ), is( 3L ) );
    }

    private File directory( String name )
    {
        File dir = new File( path, name );
        dir.mkdirs();
        return dir;
    }

    private File touchFile( String name ) throws IOException
    {
        File file = new File( path, name );
        file.createNewFile();
        return file;
    }

    private String path( String path )
    {
        return new File( path ).getPath();
    }
}
