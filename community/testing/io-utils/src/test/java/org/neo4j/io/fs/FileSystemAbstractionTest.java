/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.function.Predicates;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.fs.FileHandle.HANDLE_DELETE;
import static org.neo4j.io.fs.FileHandle.handleRename;

@TestDirectoryExtension
public abstract class FileSystemAbstractionTest
{
    @Inject
    TestDirectory testDirectory;

    private int recordSize = 9;
    private int maxPages = 20;
    private int pageCachePageSize = 32;
    private int recordsPerFilePage = pageCachePageSize / recordSize;
    private int recordCount = 25 * maxPages * recordsPerFilePage;
    protected FileSystemAbstraction fsa;
    protected File path;

    @BeforeEach
    void before()
    {
        fsa = buildFileSystemAbstraction();
        path = new File( testDirectory.homeDir(), UUID.randomUUID().toString() );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        fsa.close();
    }

    protected abstract FileSystemAbstraction buildFileSystemAbstraction();

    @Test
    void shouldCreatePath() throws Exception
    {
        fsa.mkdirs( path );

        assertTrue( fsa.fileExists( path ) );
    }

    @Test
    void shouldCreateDeepPath() throws Exception
    {
        path = new File( path, UUID.randomUUID() + "/" + UUID.randomUUID() );

        fsa.mkdirs( path );

        assertTrue( fsa.fileExists( path ) );
    }

    @Test
    void shouldCreatePathThatAlreadyExists() throws Exception
    {
        fsa.mkdirs( path );
        assertTrue( fsa.fileExists( path ) );

        fsa.mkdirs( path );

        assertTrue( fsa.fileExists( path ) );
    }

    @Test
    void shouldCreatePathThatPointsToFile() throws Exception
    {
        fsa.mkdirs( path );
        assertTrue( fsa.fileExists( path ) );
        path = new File( path, "some_file" );
        try ( StoreChannel channel = fsa.write( path ) )
        {
            assertThat( channel ).isNotNull();

            fsa.mkdirs( path );

            assertTrue( fsa.fileExists( path ) );
        }
    }

    @Test
    void moveToDirectoryMustMoveFile() throws Exception
    {
        File source = new File( path, "source" );
        File target = new File( path, "target" );
        File file = new File( source, "file" );
        File fileAfterMove = new File( target, "file" );
        fsa.mkdirs( source );
        fsa.mkdirs( target );
        fsa.write( file ).close();
        assertTrue( fsa.fileExists( file ) );
        assertFalse( fsa.fileExists( fileAfterMove ) );
        fsa.moveToDirectory( file, target );
        assertFalse( fsa.fileExists( file ) );
        assertTrue( fsa.fileExists( fileAfterMove ) );
    }

    @Test
    void copyToDirectoryCopiesFile() throws IOException
    {
        File source = new File( path, "source" );
        File target = new File( path, "target" );
        File file = new File( source, "file" );
        File fileAfterCopy = new File( target, "file" );
        fsa.mkdirs( source );
        fsa.mkdirs( target );
        fsa.write( file ).close();
        assertTrue( fsa.fileExists( file ) );
        assertFalse( fsa.fileExists( fileAfterCopy ) );
        fsa.copyToDirectory( file, target );
        assertTrue( fsa.fileExists( file ) );
        assertTrue( fsa.fileExists( fileAfterCopy ) );
    }

    @Test
    void copyToDirectoryReplaceExistingFile() throws Exception
    {
        File source = new File( path, "source" );
        File target = new File( path, "target" );
        File file = new File( source, "file" );
        File targetFile = new File( target, "file" );
        fsa.mkdirs( source );
        fsa.mkdirs( target );
        fsa.write( file ).close();

        writeIntegerIntoFile( targetFile );

        fsa.copyToDirectory( file, target );
        assertTrue( fsa.fileExists( file ) );
        assertTrue( fsa.fileExists( targetFile ) );
        assertEquals( 0L, fsa.getFileSize( targetFile ) );
    }

    @Test
    void copyFileShouldFailOnExistingTargetIfNoReplaceCopyOptionSupplied() throws Exception
    {
        // given
        fsa.mkdirs( path );
        File source = new File( path, "source" );
        File target = new File( path, "target" );
        fsa.write( source ).close();
        fsa.write( target ).close();

        // then
        assertThrows( FileAlreadyExistsException.class, () -> fsa.copyFile( source, target, FileSystemAbstraction.EMPTY_COPY_OPTIONS ) );
    }

    @Test
    void deleteRecursivelyMustDeleteAllFilesInDirectory() throws Exception
    {
        fsa.mkdirs( path );
        File a = new File( path, "a" );
        fsa.write( a ).close();
        File b = new File( path, "b" );
        fsa.write( b ).close();
        File c = new File( path, "c" );
        fsa.write( c ).close();
        File d = new File( path, "d" );
        fsa.write( d ).close();

        fsa.deleteRecursively( path );

        assertFalse( fsa.fileExists( a ) );
        assertFalse( fsa.fileExists( b ) );
        assertFalse( fsa.fileExists( c ) );
        assertFalse( fsa.fileExists( d ) );
    }

    @Test
    void deleteRecursivelyMustDeleteGivenDirectory() throws Exception
    {
        fsa.mkdirs( path );
        fsa.deleteRecursively( path );
        assertFalse( fsa.fileExists( path ) );
    }

    @Test
    void deleteRecursivelyMustDeleteGivenFile() throws Exception
    {
        fsa.mkdirs( path );
        File file = new File( path, "file" );
        fsa.write( file ).close();
        fsa.deleteRecursively( file );
        assertFalse( fsa.fileExists( file ) );
    }

    @Test
    void deleteRecursivelyMustDeleteAllSubDirectoriesInDirectory() throws IOException
    {
        fsa.mkdirs( path );
        File a = new File( path, "a" );
        fsa.mkdirs( a );
        File aa = new File( a, "a" );
        fsa.write( aa ).close();
        File b = new File( path, "b" );
        fsa.mkdirs( b );
        File c = new File( path, "c" );
        fsa.write( c ).close();
        fsa.deleteRecursively( path );

        assertFalse( fsa.fileExists( a ) );
        assertFalse( fsa.fileExists( aa ) );
        assertFalse( fsa.fileExists( b ) );
        assertFalse( fsa.fileExists( c ) );
        assertFalse( fsa.fileExists( path ) );
        assertNull( fsa.listFiles( path ) );
    }

    @Test
    void deleteRecursivelyMustNotDeleteSiblingDirectories() throws IOException
    {
        fsa.mkdirs( path );
        File a = new File( path, "a" );
        fsa.mkdirs( a );
        File b = new File( path, "b" );
        fsa.mkdirs( b );
        File bb = new File( b, "b" );
        fsa.write( bb ).close();
        File c = new File( path, "c" );
        fsa.write( c ).close();
        fsa.deleteRecursively( a );

        assertFalse( fsa.fileExists( a ) );
        assertTrue( fsa.fileExists( b ) );
        assertTrue( fsa.fileExists( bb ) );
        assertTrue( fsa.fileExists( c ) );
        assertTrue( fsa.fileExists( path ) );
    }

    @Test
    void fileWatcherCreation() throws IOException
    {
        try ( FileWatcher fileWatcher = fsa.fileWatcher() )
        {
            assertNotNull( fileWatcher.watch( testDirectory.directory( "testDirectory" ) ) );
        }
    }

    @Test
    void readAndWriteMustTakeBufferPositionIntoAccount() throws Exception
    {
        byte[] bytes = new byte[]{1, 2, 3, 4, 5};
        ByteBuffer buf = ByteBuffer.wrap( bytes );
        buf.position( 1 );

        fsa.mkdirs( path );
        File file = new File( path, "file" );
        try ( StoreChannel channel = fsa.write( file ) )
        {
            assertThat( channel.write( buf ) ).isEqualTo( 4 );
        }
        try ( InputStream stream = fsa.openAsInputStream( file ) )
        {
            assertThat( stream.read() ).isEqualTo( 2 );
            assertThat( stream.read() ).isEqualTo( 3 );
            assertThat( stream.read() ).isEqualTo( 4 );
            assertThat( stream.read() ).isEqualTo( 5 );
            assertThat( stream.read() ).isEqualTo( -1 );
        }
        Arrays.fill( bytes, (byte) 0 );
        buf.position( 1 );
        try ( StoreChannel channel = fsa.write( file ) )
        {
            assertThat( channel.read( buf ) ).isEqualTo( 4 );
            buf.clear();
            assertThat( buf.get() ).isEqualTo( (byte) 0 );
            assertThat( buf.get() ).isEqualTo( (byte) 2 );
            assertThat( buf.get() ).isEqualTo( (byte) 3 );
            assertThat( buf.get() ).isEqualTo( (byte) 4 );
            assertThat( buf.get() ).isEqualTo( (byte) 5 );
        }
    }

    @Test
    void streamFilesRecursiveMustBeEmptyForEmptyBaseDirectory() throws Exception
    {
        File dir = existingDirectory( "dir" );
        assertThat( fsa.streamFilesRecursive( dir ).count() ).isEqualTo( 0L );
    }

    @Test
    void streamFilesRecursiveMustListAllFilesInBaseDirectory() throws Exception
    {
        File a = existingFile( "a" );
        File b = existingFile( "b" );
        File c = existingFile( "c" );
        Stream<FileHandle> stream = fsa.streamFilesRecursive( a.getParentFile() );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths ).contains( a.getCanonicalFile(), b.getCanonicalFile(), c.getCanonicalFile() );
    }

    @Test
    void streamFilesRecursiveMustListAllFilesInSubDirectories() throws Exception
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
        assertThat( filepaths ).contains( a.getCanonicalFile(), b.getCanonicalFile(), c.getCanonicalFile() );
    }

    @Test
    void streamFilesRecursiveMustNotListSubDirectories() throws Exception
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
        assertThat( filepaths ).contains( a.getCanonicalFile(), b.getCanonicalFile(), c.getCanonicalFile() );
    }

    @Test
    void streamFilesRecursiveFilePathsMustBeCanonical() throws Exception
    {
        File sub = existingDirectory( "sub" );
        File a = new File( new File( new File( sub, ".." ), "sub" ), "a" );
        ensureExists( a );

        Stream<FileHandle> stream = fsa.streamFilesRecursive( sub.getParentFile() );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths ).contains( a.getCanonicalFile() );// file in our sub directory

    }

    @Test
    void streamFilesRecursiveMustBeAbleToGivePathRelativeToBase() throws Exception
    {
        File sub = existingDirectory( "sub" );
        File a = existingFile( "a" );
        File b = new File( sub, "b" );
        ensureExists( b );
        File base = a.getParentFile();
        Set<File> set = fsa.streamFilesRecursive( base ).map( FileHandle::getRelativeFile ).collect( toSet() );
        assertThat( set ).as( "Files relative to base directory " + base ).contains( new File( "a" ), new File( "sub" + File.separator + 'b' ) );
    }

    @Test
    void streamFilesRecursiveMustListSingleFileGivenAsBase() throws Exception
    {
        existingDirectory( "sub" ); // must not be observed
        existingFile( "sub/x" ); // must not be observed
        File a = existingFile( "a" );

        Stream<FileHandle> stream = fsa.streamFilesRecursive( a );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths ).contains( a ); // note that we don't go into 'sub'
    }

    @Test
    void streamFilesRecursiveListedSingleFileMustHaveCanonicalPath() throws Exception
    {
        File sub = existingDirectory( "sub" );
        existingFile( "sub/x" ); // we query specifically for 'a', so this must not be listed
        File a = existingFile( "a" );
        File queryForA = new File( new File( sub, ".." ), "a" );

        Stream<FileHandle> stream = fsa.streamFilesRecursive( queryForA );
        List<File> filepaths = stream.map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths ).contains( a.getCanonicalFile() ); // note that we don't go into 'sub'
    }

    @Test
    void streamFilesRecursiveMustReturnEmptyStreamForNonExistingBasePath() throws Exception
    {
        File nonExisting = new File( "nonExisting" );
        assertFalse( fsa.streamFilesRecursive( nonExisting ).anyMatch( Predicates.alwaysTrue() ) );
    }

    @Test
    void streamFilesRecursiveMustRenameFiles() throws Exception
    {
        File a = existingFile( "a" );
        File b = nonExistingFile( "b" ); // does not yet exist
        File base = a.getParentFile();
        fsa.streamFilesRecursive( base ).forEach( handleRename( b ) );
        List<File> filepaths = fsa.streamFilesRecursive( base ).map( FileHandle::getFile ).collect( toList() );
        assertThat( filepaths ).contains( b.getCanonicalFile() );
    }

    @Test
    void streamFilesRecursiveMustDeleteFiles() throws Exception
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

    @Test
    void streamFilesRecursiveMustThrowWhenDeletingNonExistingFile() throws Exception
    {
        File a = existingFile( "a" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        fsa.deleteFile( a );
        assertThrows( NoSuchFileException.class, handle::delete );
    }

    @Test
    void streamFilesRecursiveMustThrowWhenTargetFileOfRenameAlreadyExists() throws Exception
    {
        File a = existingFile( "a" );
        File b = existingFile( "b" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        assertThrows( FileAlreadyExistsException.class, () -> handle.rename( b ) );
    }

    @Test
    void streamFilesRecursiveMustNotThrowWhenTargetFileOfRenameAlreadyExistsAndUsingReplaceExisting()
            throws Exception
    {
        File a = existingFile( "a" );
        File b = existingFile( "b" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b, StandardCopyOption.REPLACE_EXISTING );
    }

    @Test
    void streamFilesRecursiveMustDeleteSubDirectoriesEmptiedByFileRename() throws Exception
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
    void streamFilesRecursiveMustDeleteMultipleLayersOfSubDirectoriesIfTheyBecomeEmptyByRename() throws Exception
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
    void streamFilesRecursiveMustNotDeleteDirectoriesAboveBaseDirectoryIfTheyBecomeEmptyByRename()
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
    void streamFilesRecursiveMustDeleteSubDirectoriesEmptiedByFileDelete() throws Exception
    {
        File sub = existingDirectory( "sub" );
        File x = new File( sub, "x" );
        ensureExists( x );

        fsa.streamFilesRecursive( sub ).forEach( HANDLE_DELETE );

        assertFalse( fsa.isDirectory( sub ) );
        assertFalse( fsa.fileExists( sub ) );
    }

    @Test
    void streamFilesRecursiveMustDeleteMultipleLayersOfSubDirectoriesIfTheyBecomeEmptyByDelete() throws Exception
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
    void streamFilesRecursiveMustNotDeleteDirectoriesAboveBaseDirectoryIfTheyBecomeEmptyByDelete()
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
    void streamFilesRecursiveMustCreateMissingPathDirectoriesImpliedByFileRename() throws Exception
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
    void streamFilesRecursiveMustNotSeeFilesLaterCreatedBaseDirectory() throws Exception
    {
        File a = existingFile( "a" );
        Stream<FileHandle> stream = fsa.streamFilesRecursive( a.getParentFile() );
        File b = existingFile( "b" );
        Set<File> files = stream.map( FileHandle::getFile ).collect( toSet() );
        assertThat( files ).containsExactly( a );
        assertThat( files ).doesNotContain( b );
    }

    @Test
    void streamFilesRecursiveMustNotSeeFilesRenamedIntoBaseDirectory() throws Exception
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
        assertThat( observedFiles ).contains( a, x );
    }

    @Test
    void streamFilesRecursiveMustNotSeeFilesRenamedIntoSubDirectory() throws Exception
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
        assertThat( observedFiles ).contains( a );
    }

    @Test
    void streamFilesRecursiveRenameMustCanonicaliseSourceFile() throws Exception
    {
        // File 'a' should canonicalise from 'a/poke/..' to 'a', which is a file that exists.
        // Thus, this should not throw a NoSuchFileException.
        File a = new File( new File( existingFile( "a" ), "poke" ), ".." );
        File b = nonExistingFile( "b" );

        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b ); // must not throw
    }

    @Test
    void streamFilesRecursiveRenameMustCanonicaliseTargetFile() throws Exception
    {
        // File 'b' should canonicalise from 'b/poke/..' to 'b', which is a file that doesn't exists.
        // Thus, this should not throw a NoSuchFileException for the 'poke' directory.
        File a = existingFile( "a" );
        File b = new File( new File( new File( path, "b" ), "poke" ), ".." );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
    }

    @Test
    void streamFilesRecursiveRenameTargetFileMustBeRenamed() throws Exception
    {
        File a = existingFile( "a" );
        File b = nonExistingFile( "b" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
        assertTrue( fsa.fileExists( b ) );
    }

    @Test
    void streamFilesRecursiveSourceFileMustNotBeMappableAfterRename() throws Exception
    {
        File a = existingFile( "a" );
        File b = nonExistingFile( "b" );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
        assertFalse( fsa.fileExists( a ) );

    }

    @Test
    void streamFilesRecursiveRenameMustNotChangeSourceFileContents() throws Exception
    {
        File a = existingFile( "a" );
        File b = nonExistingFile( "b" );
        generateFileWithRecords( a, recordCount );
        FileHandle handle = fsa.streamFilesRecursive( a ).findAny().get();
        handle.rename( b );
        verifyRecordsInFile( b, recordCount );
    }

    @Test
    void streamFilesRecursiveRenameMustNotChangeSourceFileContentsWithReplaceExisting() throws Exception
    {
        File a = existingFile( "a" );
        File b = existingFile( "b" );
        generateFileWithRecords( a, recordCount );
        generateFileWithRecords( b, recordCount + recordsPerFilePage );

        // Fill 'b' with random data
        try ( StoreChannel channel = fsa.write( b ) )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int fileSize = (int) channel.size();
            ByteBuffer buffer = ByteBuffers.allocate( fileSize );
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
    void lastModifiedOfNonExistingFileIsZero()
    {
        assertThat( fsa.lastModifiedTime( nonExistingFile( "blabla" ) ) ).isEqualTo( 0L );
    }

    @Test
    void shouldHandlePathThatLooksVeryDifferentWhenCanonicalized() throws Exception
    {
        File dir = existingDirectory( "/././home/.././././home/././.././././././././././././././././././home/././" );
        File a = existingFile( "/home/a" );

        List<File> filepaths = fsa.streamFilesRecursive( dir ).map( FileHandle::getRelativeFile ).collect( toList() );
        assertThat( filepaths ).contains( new File( a.getName() ) );
    }

    @Test
    void truncationMustReduceFileSize() throws Exception
    {
        File a = existingFile( "a" );
        try ( StoreChannel channel = fsa.write( a ) )
        {
            channel.position( 0 );
            byte[] data = {
                    1, 2, 3, 4,
                    5, 6, 7, 8
            };
            channel.writeAll( ByteBuffer.wrap( data ) );
            channel.truncate( 4 );
            assertThat( channel.size() ).isEqualTo( 4 );
            ByteBuffer buf = ByteBuffer.allocate( data.length );
            channel.position( 0 );
            int read = channel.read( buf );
            assertThat( read ).isEqualTo( 4 );
            buf.flip();
            assertThat( buf.remaining() ).isEqualTo( 4 );
            assertThat( buf.array() ).containsExactly( 1, 2, 3, 4, 0, 0, 0, 0 );
        }
    }

    private void generateFileWithRecords( File file, int recordCount ) throws IOException
    {
        try ( StoreChannel channel = fsa.write( file ) )
        {
            ByteBuffer buf = ByteBuffers.allocate( recordSize );
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
        try ( StoreChannel channel = fsa.write( file ) )
        {
            ByteBuffer buf = ByteBuffers.allocate( recordSize );
            ByteBuffer observation = ByteBuffers.allocate( recordSize );
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
        assertThat( actualBytes ).as( "Page id: " + pageId + ' ' + "(based on record data, it should have been " + estimatedPageId + ", a difference of " +
                Math.abs( pageId - estimatedPageId ) + ')' ).containsExactly( expectedBytes );
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
        fsa.write( file ).close();
        return file;
    }

    private File nonExistingFile( String fileName )
    {
        return new File( path, fileName );
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
        fsa.write( file ).close();
    }

    private void ensureDirectoryExists( File directory ) throws IOException
    {
        fsa.mkdirs( directory );
    }

    private void writeIntegerIntoFile( File targetFile ) throws IOException
    {
        StoreChannel storeChannel = fsa.write( targetFile );
        ByteBuffer byteBuffer = ByteBuffers.allocate( Integer.SIZE ).putInt( 7 );
        byteBuffer.flip();
        storeChannel.writeAll( byteBuffer );
        storeChannel.close();
    }
}
