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
package org.neo4j.dbms.archive;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.neo4j.dbms.archive.TestUtils.withPermissions;

@ExtendWith( TestDirectoryExtension.class )
class LoaderTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldGiveAClearErrorMessageIfTheArchiveDoesntExist()
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.file( "the-destination" ).toPath();
        NoSuchFileException exception = assertThrows( NoSuchFileException.class, () -> new Loader().load( archive, destination, destination ) );
        assertEquals( archive.toString(), exception.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheArchiveIsNotInGzipFormat() throws IOException
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Files.write( archive, singletonList( "some incorrectly formatted data" ) );
        Path destination = testDirectory.file( "the-destination" ).toPath();
        IncorrectFormat incorrectFormat = assertThrows( IncorrectFormat.class, () -> new Loader().load( archive, destination, destination ) );
        assertEquals( archive.toString(), incorrectFormat.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheArchiveIsNotInTarFormat() throws IOException
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        try ( GzipCompressorOutputStream compressor =
                      new GzipCompressorOutputStream( Files.newOutputStream( archive ) ) )
        {
            byte[] bytes = new byte[1000];
            new Random().nextBytes( bytes );
            compressor.write( bytes );
        }

        Path destination = testDirectory.file( "the-destination" ).toPath();

        IncorrectFormat incorrectFormat = assertThrows( IncorrectFormat.class, () -> new Loader().load( archive, destination, destination ) );
        assertEquals( archive.toString(), incorrectFormat.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheArchiveEntryPointsToRandomPlace() throws IOException, IncorrectFormat
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        final File testFile = testDirectory.file( "testFile" );
        try ( TarArchiveOutputStream tar = new TarArchiveOutputStream(
                new GzipCompressorOutputStream( Files.newOutputStream( archive, StandardOpenOption.CREATE_NEW ) ) ) )
        {
            ArchiveEntry archiveEntry = tar.createArchiveEntry( testFile, "../../../../etc/shadow" );
            tar.putArchiveEntry( archiveEntry );
            tar.closeArchiveEntry();
        }
        Path destination = testDirectory.file( "the-destination" ).toPath();
        final InvalidDumpEntryException exception =
                assertThrows( InvalidDumpEntryException.class, () -> new Loader().load( archive, destination, destination ) );
        assertThat( exception.getMessage(), containsString( "points to a location outside of the destination database." ) );
    }

    @Test
    void shouldGiveAClearErrorIfTheDestinationAlreadyExists()
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.directory( "the-destination" ).toPath();
        FileAlreadyExistsException exception = assertThrows( FileAlreadyExistsException.class, () -> new Loader().load( archive, destination, destination ) );
        assertEquals( destination.toString(), exception.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorIfTheDestinationTxLogAlreadyExists()
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.file( "the-destination" ).toPath();
        Path txLogsDestination = testDirectory.directory( "txLogsDestination" ).toPath();

        FileAlreadyExistsException exception =
                assertThrows( FileAlreadyExistsException.class, () -> new Loader().load( archive, destination, txLogsDestination ) );
        assertEquals( txLogsDestination.toString(), exception.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheDestinationsParentDirectoryDoesntExist() throws IOException
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = Paths.get( testDirectory.absolutePath().getAbsolutePath(), "subdir", "the-destination" );
        NoSuchFileException noSuchFileException = assertThrows( NoSuchFileException.class, () -> new Loader().load( archive, destination, destination ) );
        assertEquals( destination.getParent().toString(), noSuchFileException.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheTxLogsParentDirectoryDoesntExist()
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.file( "destination" ).toPath();
        Path txLogsDestination = Paths.get( testDirectory.absolutePath().getAbsolutePath(), "subdir", "txLogs" );
        NoSuchFileException noSuchFileException = assertThrows( NoSuchFileException.class, () -> new Loader().load( archive, destination, txLogsDestination ) );
        assertEquals( txLogsDestination.getParent().toString(), noSuchFileException.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheDestinationsParentDirectoryIsAFile()
            throws IOException
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = Paths.get( testDirectory.absolutePath().getAbsolutePath(), "subdir", "the-destination" );
        Files.write( destination.getParent(), new byte[0] );
        FileSystemException exception = assertThrows( FileSystemException.class, () -> new Loader().load( archive, destination, destination ) );
        assertEquals( destination.getParent().toString() + ": Not a directory", exception.getMessage() );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldGiveAClearErrorMessageIfTheDestinationsParentDirectoryIsNotWritable()
            throws IOException
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.directory( "subdir/the-destination" ).toPath();
        Files.createDirectories( destination.getParent() );
        try ( Closeable ignored = withPermissions( destination.getParent(), emptySet() ) )
        {
            assumeFalse( destination.getParent().toFile().canWrite() );
            AccessDeniedException exception = assertThrows( AccessDeniedException.class, () -> new Loader().load( archive, destination, destination ) );
            assertEquals( destination.getParent().toString(), exception.getMessage() );
        }
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldGiveAClearErrorMessageIfTheTxLogsParentDirectoryIsNotWritable()
            throws IOException
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.file( "destination" ).toPath();
        Path txLogsDirectory = testDirectory.directory( "subdir/txLogs" ).toPath();
        Files.createDirectories( txLogsDirectory.getParent() );
        try ( Closeable ignored = withPermissions( txLogsDirectory.getParent(), emptySet() ) )
        {
            assumeFalse( txLogsDirectory.getParent().toFile().canWrite() );
            AccessDeniedException exception = assertThrows( AccessDeniedException.class, () -> new Loader().load( archive, destination, txLogsDirectory ) );
            assertEquals( txLogsDirectory.getParent().toString(), exception.getMessage() );
        }
    }
}
