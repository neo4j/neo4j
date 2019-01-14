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
package org.neo4j.io.fs;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.mockfs.CloseTrackingFileSystem;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.io.fs.FileHandle.HANDLE_DELETE;
import static org.neo4j.io.fs.FileHandle.handleRename;
import static org.neo4j.test.matchers.ByteArrayMatcher.byteArray;

public abstract class FileSystemAbstractionTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private int recordSize = 9;
    private int maxPages = 20;
    private int pageCachePageSize = 32;
    private int recordsPerFilePage = pageCachePageSize / recordSize;
    private int recordCount = 25 * maxPages * recordsPerFilePage;
    private int filePageSize = recordsPerFilePage * recordSize;
    protected FileSystemAbstraction fsa;
    protected File path;

    @Before
    public void before()
    {
        fsa = buildFileSystemAbstraction();
        path = new File( testDirectory.directory(), UUID.randomUUID().toString() );
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
        fsa.mkdirs( path );
        assertTrue( fsa.fileExists( path ) );

        fsa.mkdirs( path );

        assertTrue( fsa.fileExists( path ) );
    }

    @Test
    public void shouldCreatePathThatPointsToFile() throws Exception
    {
        fsa.mkdirs( path );
        assertTrue( fsa.fileExists( path ) );
        path = new File( path, "some_file" );
        try ( StoreChannel channel = fsa.create( path ) )
        {
            assertThat( channel, is( not( nullValue() ) ) );

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
    public void copyToDirectoryCopiesFile() throws IOException
    {
        File source = new File( path, "source" );
        File target = new File( path, "target" );
        File file = new File( source, "file" );
        File fileAfterCopy = new File( target, "file" );
        fsa.mkdirs( source );
        fsa.mkdirs( target );
        fsa.create( file ).close();
        assertTrue( fsa.fileExists( file ) );
        assertFalse( fsa.fileExists( fileAfterCopy ) );
        fsa.copyToDirectory( file, target );
        assertTrue( fsa.fileExists( file ) );
        assertTrue( fsa.fileExists( fileAfterCopy ) );
    }

    @Test
    public void copyToDirectoryReplaceExistingFile() throws Exception
    {
        File source = new File( path, "source" );
        File target = new File( path, "target" );
        File file = new File( source, "file" );
        File targetFile = new File( target, "file" );
        fsa.mkdirs( source );
        fsa.mkdirs( target );
        fsa.create( file ).close();

        writeIntegerIntoFile( targetFile );

        fsa.copyToDirectory( file, target );
        assertTrue( fsa.fileExists( file ) );
        assertTrue( fsa.fileExists( targetFile ) );
        assertEquals( 0L, fsa.getFileSize( targetFile ) );
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
            assertNotNull( fileWatcher.watch( testDirectory.directory( "testDirectory" ) ) );
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
        byte[] bytes = new byte[]{1, 2, 3, 4, 5};
        ByteBuffer buf = ByteBuffer.wrap( bytes );
        buf.position( 1 );

        fsa.mkdirs( path );
        File file = new File( path, "file" );
        try ( StoreChannel channel = fsa.open( file, OpenMode.READ_WRITE ) )
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
        try ( StoreChannel channel = fsa.open( file, OpenMode.READ_WRITE ) )
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

    @Test
    public void streamFilesRecursiveMustBeEmptyForEmptyBaseDirectory() throws Exception
    {
        File dir = existingDirectory( "dir" );
        assertThat( fsa.streamFilesRecursive( dir ).count(), Matchers.is( 0L ) );
    }

    @Test
    public void streamFilesRecursiveMustListAllFilesInBaseDirectory() throws Exception
    {
        File a = existingFile( "a" );
        File b = existingFile( "b" );
        File c = existingFile( "c" );
        Stream<FileHandle> stream = fsa.streamFilesRecursive( a.getParentFile() );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths, containsInAnyOrder( a.getCanonicalFile(), b.getCanonicalFile(), c.getCanonicalFile() ) );
    }

    @Test
    public void streamFilesRecursiveMustListAllFilesInSubDirectories() throws Exception
    {
        File sub1 = existingDirectory( "sub1" );
        File sub2 = existingDirectory( "sub2" );
        File a = existingFile( "a" );
        File b = new File( sub1, "b" );
        File c = new File( sub2, "c" );
        ensureExists( b );
        ensureExists( c );

        Stream<FileHandle> stream = fsa.streamFilesRecursive( a.getParentFile() );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths, containsInAnyOrder( a.getCanonicalFile(), b.getCanonicalFile(), c.getCanonicalFile() ) );
    }

    @Test
    public void streamFilesRecursiveMustNotListSubDirectories() throws Exception
    {
        File sub1 = existingDirectory( "sub1" );
        File sub2 = existingDirectory( "sub2" );
        File sub2sub1 = new File( sub2, "sub1" );
        ensureDirectoryExists( sub2sub1 );
        existingDirectory( "sub3" ); // must not be observed in the stream
        File a = existingFile( "a" );
        File b = new File( sub1, "b" );
        File c = new File( sub2, "c" );
        ensureExists( b );
        ensureExists( c );

        Stream<FileHandle> stream = fsa.streamFilesRecursive( a.getParentFile() );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths, containsInAnyOrder( a.getCanonicalFile(), b.getCanonicalFile(), c.getCanonicalFile() ) );
    }

    @Test
    public void streamFilesRecursiveFilePathsMustBeCanonical() throws Exception
    {
        File sub = existingDirectory( "sub" );
        File a = new File( new File( new File( sub, ".." ), "sub" ), "a" );
        ensureExists( a );

        Stream<FileHandle> stream = fsa.streamFilesRecursive( sub.getParentFile() );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths, containsInAnyOrder( a.getCanonicalFile() ) );// file in our sub directory

    }

    @Test
    public void streamFilesRecursiveMustBeAbleToGivePathRelativeToBase() throws Exception
    {
        File sub = existingDirectory( "sub" );
        File a = existingFile( "a" );
        File b = new File( sub, "b" );
        ensureExists( b );
        File base = a.getParentFile();
        Set<File> set = fsa.streamFilesRecursive( base ).map( FileHandle::getRelativeFile ).collect( toSet() );
        assertThat( "Files relative to base directory " + base, set,
                containsInAnyOrder( new File( "a" ), new File( "sub" + File.separator + "b" ) ) );
    }

    @Test
    public void streamFilesRecursiveMustListSingleFileGivenAsBase() throws Exception
    {
        existingDirectory( "sub" ); // must not be observed
        existingFile( "sub/x" ); // must not be observed
        File a = existingFile( "a" );

        Stream<FileHandle> stream = fsa.streamFilesRecursive( a );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths, containsInAnyOrder( a ) ); // note that we don't go into 'sub'
    }

    @Test
    public void streamFilesRecursiveListedSingleFileMustHaveCanonicalPath() throws Exception
    {
        File sub = existingDirectory( "sub" );
        existingFile( "sub/x" ); // we query specifically for 'a', so this must not be listed
        File a = existingFile( "a" );
        File queryForA = new File( new File( sub, ".." ), "a" );

        Stream<FileHandle> stream = fsa.streamFilesRecursive( queryForA );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths, containsInAnyOrder( a.getCanonicalFile() ) ); // note that we don't go into 'sub'
    }

    @Test
    public void streamFilesRecursiveMustReturnEmptyStreamForNonExistingBasePath() throws Exception
    {
        File nonExisting = new File( "nonExisting" );
        assertFalse( fsa.streamFilesRecursive( nonExisting ).anyMatch( Predicates.alwaysTrue() ) );
    }

    @Test
    public void streamFilesRecursiveMustRenameFiles() throws Exception
    {
        File a = existingFile( "a" );
        File b = nonExistingFile( "b" ); // does not yet exist
        File base = a.getParentFile();
        fsa.streamFilesRecursive( base ).forEach( handleRename( b ) );
        List<File> filepaths = fsa.streamFilesRecursive( base ).map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths, containsInAnyOrder( b.getCanonicalFile() ) );
    }

    @Test
    public void streamFilesRecursiveMustDeleteFiles() throws Exception
    {
        File a = existingFile( "a" );
        File b = existingFile( "b" );
        File c = existingFile( "c" );

        File base = a.getParentFile();
        fsa.streamFilesRecursive( base ).forEach( HANDLE_DELETE );

        assertFalse( fsa.fileExists( a ) );
        assertFalse( fsa.fileExists( b ) );
        assertFalse( fsa.fileExists( c ) );
    }

    private Predicate<FileHandle> hasFile( File a )
    {
        return fh -> fh.getFile().equals( a );
    }

    @Test
    public void streamFilesRecursiveMustThrowWhenDeletingNonExistingFile() throws Exception
    {
        File a = existingFile( "a" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        fsa.deleteFile( a );
        expectedException.expect( NoSuchFileException.class );
        handle.delete(); // must throw
    }

    @Test
    public void streamFilesRecursiveMustThrowWhenTargetFileOfRenameAlreadyExists() throws Exception
    {
        File a = existingFile( "a" );
        File b = existingFile( "b" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        expectedException.expect( FileAlreadyExistsException.class );
        handle.rename( b );
    }

    @Test
    public void streamFilesRecursiveMustNotThrowWhenTargetFileOfRenameAlreadyExistsAndUsingReplaceExisting()
            throws Exception
    {
        File a = existingFile( "a" );
        File b = existingFile( "b" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b, StandardCopyOption.REPLACE_EXISTING );
    }

    @Test
    public void streamFilesRecursiveMustDeleteSubDirectoriesEmptiedByFileRename() throws Exception
    {
        File sub = existingDirectory( "sub" );
        File x = new File( sub, "x" );
        ensureExists( x );
        File target = nonExistingFile( "target" );

        fsa.streamFilesRecursive( sub ).forEach( handleRename( target ) );

        assertFalse( fsa.isDirectory( sub ) );
        assertFalse( fsa.fileExists( sub ) );
    }

    @Test
    public void streamFilesRecursiveMustDeleteMultipleLayersOfSubDirectoriesIfTheyBecomeEmptyByRename() throws Exception
    {
        File sub = existingDirectory( "sub" );
        File subsub = new File( sub, "subsub" );
        ensureDirectoryExists( subsub );
        File x = new File( subsub, "x" );
        ensureExists( x );
        File target = nonExistingFile( "target" );

        fsa.streamFilesRecursive( sub ).forEach( handleRename( target ) );

        assertFalse( fsa.isDirectory( subsub ) );
        assertFalse( fsa.fileExists( subsub ) );
        assertFalse( fsa.isDirectory( sub ) );
        assertFalse( fsa.fileExists( sub ) );
    }

    @Test
    public void streamFilesRecursiveMustNotDeleteDirectoriesAboveBaseDirectoryIfTheyBecomeEmptyByRename()
            throws Exception
    {
        File sub = existingDirectory( "sub" );
        File subsub = new File( sub, "subsub" );
        File subsubsub = new File( subsub, "subsubsub" );
        ensureDirectoryExists( subsub );
        ensureDirectoryExists( subsubsub );
        File x = new File( subsubsub, "x" );
        ensureExists( x );
        File target = nonExistingFile( "target" );

        fsa.streamFilesRecursive( subsub ).forEach( handleRename( target ) );

        assertFalse( fsa.fileExists( subsubsub ) );
        assertFalse( fsa.isDirectory( subsubsub ) );
        assertFalse( fsa.fileExists( subsub ) );
        assertFalse( fsa.isDirectory( subsub ) );
        assertTrue( fsa.fileExists( sub ) );
        assertTrue( fsa.isDirectory( sub ) );
    }

    @Test
    public void streamFilesRecursiveMustDeleteSubDirectoriesEmptiedByFileDelete() throws Exception
    {
        File sub = existingDirectory( "sub" );
        File x = new File( sub, "x" );
        ensureExists( x );

        fsa.streamFilesRecursive( sub ).forEach( HANDLE_DELETE );

        assertFalse( fsa.isDirectory( sub ) );
        assertFalse( fsa.fileExists( sub ) );
    }

    @Test
    public void streamFilesRecursiveMustDeleteMultipleLayersOfSubDirectoriesIfTheyBecomeEmptyByDelete() throws Exception
    {
        File sub = existingDirectory( "sub" );
        File subsub = new File( sub, "subsub" );
        ensureDirectoryExists( subsub );
        File x = new File( subsub, "x" );
        ensureExists( x );

        fsa.streamFilesRecursive( sub ).forEach( HANDLE_DELETE );

        assertFalse( fsa.isDirectory( subsub ) );
        assertFalse( fsa.fileExists( subsub ) );
        assertFalse( fsa.isDirectory( sub ) );
        assertFalse( fsa.fileExists( sub ) );
    }

    @Test
    public void streamFilesRecursiveMustNotDeleteDirectoriesAboveBaseDirectoryIfTheyBecomeEmptyByDelete()
            throws Exception
    {
        File sub = existingDirectory( "sub" );
        File subsub = new File( sub, "subsub" );
        File subsubsub = new File( subsub, "subsubsub" );
        ensureDirectoryExists( subsub );
        ensureDirectoryExists( subsubsub );
        File x = new File( subsubsub, "x" );
        ensureExists( x );

        fsa.streamFilesRecursive( subsub ).forEach( HANDLE_DELETE );

        assertFalse( fsa.fileExists( subsubsub ) );
        assertFalse( fsa.isDirectory( subsubsub ) );
        assertFalse( fsa.fileExists( subsub ) );
        assertFalse( fsa.isDirectory( subsub ) );
        assertTrue( fsa.fileExists( sub ) );
        assertTrue( fsa.isDirectory( sub ) );
    }

    @Test
    public void streamFilesRecursiveMustCreateMissingPathDirectoriesImpliedByFileRename() throws Exception
    {
        File a = existingFile( "a" );
        File sub = new File( path, "sub" ); // does not exists
        File target = new File( sub, "b" );

        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( target );

        assertTrue( fsa.isDirectory( sub ) );
        assertTrue( fsa.fileExists( target ) );
    }

    @Test
    public void streamFilesRecursiveMustNotSeeFilesLaterCreatedBaseDirectory() throws Exception
    {
        File a = existingFile( "a" );
        Stream<FileHandle> stream = fsa.streamFilesRecursive( a.getParentFile() );
        File b = existingFile( "b" );
        Set<File> files = stream.map( FileHandle::getFile ).collect( toSet() );
        assertThat( files, contains( a ) );
        assertThat( files, not( contains( b ) ) );
    }

    @Test
    public void streamFilesRecursiveMustNotSeeFilesRenamedIntoBaseDirectory() throws Exception
    {
        File a = existingFile( "a" );
        File sub = existingDirectory( "sub" );
        File x = new File( sub, "x" );
        ensureExists( x );
        File target = nonExistingFile( "target" );
        Set<File> observedFiles = new HashSet<>();
        fsa.streamFilesRecursive( a.getParentFile() ).forEach( fh ->
        {
            File file = fh.getFile();
            observedFiles.add( file );
            if ( file.equals( x ) )
            {
                handleRename( target ).accept( fh );
            }
        } );
        assertThat( observedFiles, containsInAnyOrder( a, x ) );
    }

    @Test
    public void streamFilesRecursiveMustNotSeeFilesRenamedIntoSubDirectory() throws Exception
    {
        File a = existingFile( "a" );
        File sub = existingDirectory( "sub" );
        File target = new File( sub, "target" );
        Set<File> observedFiles = new HashSet<>();
        fsa.streamFilesRecursive( a.getParentFile() ).forEach( fh ->
        {
            File file = fh.getFile();
            observedFiles.add( file );
            if ( file.equals( a ) )
            {
                handleRename( target ).accept( fh );
            }
        } );
        assertThat( observedFiles, containsInAnyOrder( a ) );
    }

    @Test
    public void streamFilesRecursiveRenameMustCanonicaliseSourceFile() throws Exception
    {
        // File 'a' should canonicalise from 'a/poke/..' to 'a', which is a file that exists.
        // Thus, this should not throw a NoSuchFileException.
        File a = new File( new File( existingFile( "a" ), "poke" ), ".." );
        File b = nonExistingFile( "b" );

        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b ); // must not throw
    }

    @Test
    public void streamFilesRecursiveRenameMustCanonicaliseTargetFile() throws Exception
    {
        // File 'b' should canonicalise from 'b/poke/..' to 'b', which is a file that doesn't exists.
        // Thus, this should not throw a NoSuchFileException for the 'poke' directory.
        File a = existingFile( "a" );
        File b = new File( new File( new File( path, "b" ), "poke" ), ".." );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
    }

    @Test
    public void streamFilesRecursiveRenameTargetFileMustBeRenamed() throws Exception
    {
        File a = existingFile( "a" );
        File b = nonExistingFile( "b" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
        assertTrue( fsa.fileExists( b ) );
    }

    @Test
    public void streamFilesRecursiveSourceFileMustNotBeMappableAfterRename() throws Exception
    {
        File a = existingFile( "a" );
        File b = nonExistingFile( "b" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
        assertFalse( fsa.fileExists( a ) );

    }

    @Test
    public void streamFilesRecursiveRenameMustNotChangeSourceFileContents() throws Exception
    {
        File a = existingFile( "a" );
        File b = nonExistingFile( "b" );
        generateFileWithRecords( a, recordCount );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
        verifyRecordsInFile( b, recordCount );
    }

    @Test
    public void streamFilesRecursiveRenameMustNotChangeSourceFileContentsWithReplaceExisting() throws Exception
    {
        File a = existingFile( "a" );
        File b = existingFile( "b" );
        generateFileWithRecords( a, recordCount );
        generateFileWithRecords( b, recordCount + recordsPerFilePage );

        // Fill 'b' with random data
        try ( StoreChannel channel = fsa.open( b, OpenMode.READ_WRITE ) )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int fileSize = (int) channel.size();
            ByteBuffer buffer = ByteBuffer.allocate( fileSize );
            for ( int i = 0; i < fileSize; i++ )

            {
                buffer.put( i, (byte) rng.nextInt() );
            }
            buffer.rewind();
            channel.writeAll( buffer );
        }

        // Do the rename
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b, REPLACE_EXISTING );

        // Then verify that the old random data we put in 'b' has been replaced with the contents of 'a'
        verifyRecordsInFile( b, recordCount );

    }

    @Test
    public void lastModifiedOfNonExistingFileIsZero() throws Exception
    {
        assertThat( fsa.lastModifiedTime( nonExistingFile( "blabla" ) ), is( 0L ) );
    }

    @Test
    public void shouldHandlePathThatLooksVeryDifferentWhenCanonicalized() throws Exception
    {
        File dir = existingDirectory( "/././home/.././././home/././.././././././././././././././././././home/././" );
        File a = existingFile( "/home/a" );

        List<File> filepaths = fsa.streamFilesRecursive( dir ).map( FileHandle::getRelativeFile ).collect( toList() );
        assertThat( filepaths, containsInAnyOrder( new File( a.getName() ) ) );
    }

    private void generateFileWithRecords( File file, int recordCount ) throws IOException
    {
        try ( StoreChannel channel = fsa.open( file, OpenMode.READ_WRITE ) )
        {
            ByteBuffer buf = ByteBuffer.allocate( recordSize );
            for ( int i = 0; i < recordCount; i++ )
            {
                generateRecordForId( i, buf );
                int rem = buf.remaining();
                do
                {
                    rem -= channel.write( buf );
                }
                while ( rem > 0 );
            }
        }
    }

    private void verifyRecordsInFile( File file, int recordCount ) throws IOException
    {
        try ( StoreChannel channel = fsa.open( file, OpenMode.READ ) )
        {
            ByteBuffer buf = ByteBuffer.allocate( recordSize );
            ByteBuffer observation = ByteBuffer.allocate( recordSize );
            for ( int i = 0; i < recordCount; i++ )
            {
                generateRecordForId( i, buf );
                observation.position( 0 );
                channel.read( observation );
                assertRecord( i, observation, buf );
            }
        }
    }

    private void assertRecord( long pageId, ByteBuffer actualPageContents, ByteBuffer expectedPageContents )
    {
        byte[] actualBytes = actualPageContents.array();
        byte[] expectedBytes = expectedPageContents.array();
        int estimatedPageId = estimateId( actualBytes );
        assertThat( "Page id: " + pageId + " " + "(based on record data, it should have been " + estimatedPageId +
                    ", a difference of " + Math.abs( pageId - estimatedPageId ) + ")", actualBytes,
                byteArray( expectedBytes ) );
    }

    private int estimateId( byte[] record )
    {
        return ByteBuffer.wrap( record ).getInt() - 1;
    }

    private static void generateRecordForId( long id, ByteBuffer buf )
    {
        buf.position( 0 );
        int x = (int) (id + 1);
        buf.putInt( x );
        while ( buf.position() < buf.limit() )
        {
            x++;
            buf.put( (byte) (x & 0xFF) );
        }
        buf.position( 0 );
    }

    private File existingFile( String fileName ) throws IOException
    {
        File file = new File( path, fileName );
        fsa.mkdirs( path );
        fsa.create( file ).close();
        return file;
    }

    private File nonExistingFile( String fileName )
    {
        File file = new File( path, fileName );
        return file;
    }

    private File existingDirectory( String dir ) throws IOException
    {
        File directory = new File( path, dir );
        fsa.mkdirs( directory );
        return directory;
    }

    private void ensureExists( File file ) throws IOException
    {
        fsa.mkdirs( file.getParentFile() );
        fsa.create( file ).close();
    }

    private void ensureDirectoryExists( File directory ) throws IOException
    {
        fsa.mkdirs( directory );
    }

    private void writeIntegerIntoFile( File targetFile ) throws IOException
    {
        StoreChannel storeChannel = fsa.create( targetFile );
        ByteBuffer byteBuffer = ByteBuffer.allocate( Integer.SIZE ).putInt( 7 );
        byteBuffer.flip();
        storeChannel.writeAll( byteBuffer );
        storeChannel.close();
    }
}
