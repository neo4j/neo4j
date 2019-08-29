/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.io.fs.FileUtils.pathToFileAfterMove;

@TestDirectoryExtension
class FileUtilsTest
{
    @Inject
    private TestDirectory testDirectory;

    private File path;

    @BeforeEach
    void beforeEach()
    {
        path = testDirectory.directory( "path" );
    }

    @Test
    void moveFileToDirectory() throws Exception
    {
        File file = touchFile( "source" );
        File targetDir = directory( "dir" );

        File newLocationOfFile = FileUtils.moveFileToDirectory( file, targetDir );
        assertTrue( newLocationOfFile.exists() );
        assertFalse( file.exists() );
        File[] files = targetDir.listFiles();
        assertNotNull( files );
        assertEquals( newLocationOfFile, files[0] );
    }

    @Test
    void moveFile() throws Exception
    {
        File file = touchFile( "source" );
        File targetDir = directory( "dir" );

        File newLocationOfFile = new File( targetDir, "new-name" );
        FileUtils.moveFile( file, newLocationOfFile );
        assertTrue( newLocationOfFile.exists() );
        assertFalse( file.exists() );
        File[] files = targetDir.listFiles();
        assertNotNull( files );
        assertEquals( newLocationOfFile, files[0] );
    }

    @Test
    void deletePathRecursively() throws IOException
    {
        File root = testDirectory.directory( "a" );
        File child = new File( root, "b" );
        File file = new File( child, "c" );

        assertTrue( child.mkdirs() );
        assertTrue( file.createNewFile() );

        FileUtils.deletePathRecursively( root.toPath() );

        assertFalse( file.exists() );
        assertFalse( child.exists() );
    }

    @Test
    void deletePathRecursivelyWithFilter() throws IOException
    {
        File root = testDirectory.directory( "a" );
        File child = new File( root, "b" );
        File file = new File( child, "c" );

        File toKeepDir = new File( root, "d" );
        File toKeepFile = new File( toKeepDir, "e" );

        assertTrue( child.mkdirs() );
        assertTrue( file.createNewFile() );
        assertTrue( toKeepDir.mkdirs() );
        assertTrue( toKeepFile.createNewFile() );

        FileUtils.deletePathRecursively( root.toPath(), path -> !path.equals( toKeepFile.toPath() ) );

        assertFalse( file.exists() );
        assertFalse( child.exists() );

        assertTrue( toKeepFile.exists() );
        assertTrue( toKeepDir.exists() );
    }

    @Test
    void deleteNestedPathRecursivelyWithFilter() throws IOException
    {
        File root = testDirectory.directory( "a" );
        File child = new File( root, "a" );
        File file = new File( child, "aaFile" );

        File toKeepDelete = new File( root, "b" );

        assertTrue( child.mkdirs() );
        assertTrue( file.createNewFile() );
        assertTrue( toKeepDelete.mkdirs() );

        FileUtils.deletePathRecursively( root.toPath(), path -> !path.equals( file.toPath() ) );

        assertTrue( file.exists() );
        assertTrue( child.exists() );

        assertFalse( toKeepDelete.exists() );
    }

    @Test
    void pathToFileAfterMoveMustThrowIfFileNotSubPathToFromShorter()
    {
        File file = new File( "/a" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        assertThrows( IllegalArgumentException.class, () -> pathToFileAfterMove( from, to, file ) );
    }

    // INVALID
    @Test
    void pathToFileAfterMoveMustThrowIfFileNotSubPathToFromSameLength()
    {
        File file = new File( "/a/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        assertThrows( IllegalArgumentException.class, () -> pathToFileAfterMove( from, to, file ) );
    }

    @Test
    void pathToFileAfterMoveMustThrowIfFileNotSubPathToFromLonger()
    {
        File file = new File( "/a/c/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        assertThrows( IllegalArgumentException.class, () -> pathToFileAfterMove( from, to, file ) );
    }

    @Test
    void pathToFileAfterMoveMustThrowIfFromDirIsCompletePathToFile()
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b/f" );
        File to   = new File( "/a/c" );

        assertThrows( IllegalArgumentException.class, () -> pathToFileAfterMove( from, to, file ) );
    }

    // SIBLING
    @Test
    void pathToFileAfterMoveMustWorkIfMovingToSibling()
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/c/f" ) ) );
    }

    @Test
    void pathToFileAfterMoveMustWorkIfMovingToSiblingAndFileHasSubDir()
    {
        File file = new File( "/a/b/d/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/c/d/f" ) ) );
    }

    // DEEPER
    @Test
    void pathToFileAfterMoveMustWorkIfMovingToSubDir()
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/b/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/b/c/f" ) ) );
    }

    @Test
    void pathToFileAfterMoveMustWorkIfMovingToSubDirAndFileHasSubDir()
    {
        File file = new File( "/a/b/d/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/b/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/b/c/d/f" ) ) );
    }

    @Test
    void pathToFileAfterMoveMustWorkIfMovingOutOfDir()
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/c/f" ) ) );
    }

    @Test
    void pathToFileAfterMoveMustWorkIfMovingOutOfDirAndFileHasSubDir()
    {
        File file = new File( "/a/b/d/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/c" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/c/d/f" ) ) );
    }

    @Test
    void pathToFileAfterMoveMustWorkIfNotMovingAtAll()
    {
        File file = new File( "/a/b/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/b" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/b/f" ) ) );
    }

    @Test
    void pathToFileAfterMoveMustWorkIfNotMovingAtAllAndFileHasSubDir()
    {
        File file = new File( "/a/b/d/f" );
        File from = new File( "/a/b" );
        File to   = new File( "/a/b" );

        assertThat( pathToFileAfterMove( from, to, file ).getPath(), is( path( "/a/b/d/f" ) ) );
    }

    @Test
    void allMacsHaveHighIO()
    {
        assumeTrue( SystemUtils.IS_OS_MAC );
        assertTrue( FileUtils.highIODevice( Paths.get( "." ) ) );
    }

    @Test
    void allWindowsHaveHighIO()
    {
        assumeTrue( SystemUtils.IS_OS_WINDOWS );
        assertTrue( FileUtils.highIODevice( Paths.get( "." ) ) );
    }

    @Test
    void onLinuxDevShmHasHighIO()
    {
        assumeTrue( SystemUtils.IS_OS_LINUX );
        assertTrue( FileUtils.highIODevice( Paths.get( "/dev/shm" ) ) );
    }

    @Test
    void mustCountDirectoryContents() throws Exception
    {
        File dir = directory( "dir" );
        File file = new File( dir, "file" );
        File subdir = new File( dir, "subdir" );
        assertTrue( file.createNewFile() );
        assertTrue( subdir.mkdirs() );

        assertThat( FileUtils.countFilesInDirectoryPath( dir.toPath() ), is( 2L ) );
    }

    private File directory( String name )
    {
        File dir = new File( path, name );
        assertTrue( dir.mkdirs() );
        return dir;
    }

    private File touchFile( String name ) throws IOException
    {
        File file = new File( path, name );
        assertTrue( file.createNewFile() );
        return file;
    }

    private String path( String path )
    {
        return new File( path ).getPath();
    }
}
