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
package org.neo4j.dbms.archive;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Random;

import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.neo4j.dbms.archive.TestUtils.withPermissions;

public class LoaderTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldGiveAClearErrorMessageIfTheArchiveDoesntExist() throws IOException, IncorrectFormat
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.file( "the-destination" ).toPath();
        try
        {
            new Loader().load( archive, destination, destination );
            fail( "Expected an exception" );
        }
        catch ( NoSuchFileException e )
        {
            assertEquals( archive.toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheArchiveIsNotInGzipFormat() throws IOException
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Files.write( archive, asList( "some incorrectly formatted data" ) );
        Path destination = testDirectory.file( "the-destination" ).toPath();
        try
        {
            new Loader().load( archive, destination, destination );
            fail( "Expected an exception" );
        }
        catch ( IncorrectFormat e )
        {
            assertEquals( archive.toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheArchiveIsNotInTarFormat() throws IOException
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
        try
        {
            new Loader().load( archive, destination, destination );
            fail( "Expected an exception" );
        }
        catch ( IncorrectFormat e )
        {
            assertEquals( archive.toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorIfTheDestinationAlreadyExists() throws IOException, IncorrectFormat
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.directory( "the-destination" ).toPath();
        try
        {
            new Loader().load( archive, destination, destination );
            fail( "Expected an exception" );
        }
        catch ( FileAlreadyExistsException e )
        {
            assertEquals( destination.toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorIfTheDestinationTxLogAlreadyExists() throws IOException, IncorrectFormat
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.file( "the-destination" ).toPath();
        Path txLogsDestination = testDirectory.directory( "txLogsDestination" ).toPath();
        try
        {
            new Loader().load( archive, destination, txLogsDestination );
            fail( "Expected an exception" );
        }
        catch ( FileAlreadyExistsException e )
        {
            assertEquals( txLogsDestination.toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheDestinationsParentDirectoryDoesntExist()
            throws IOException, IncorrectFormat
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.directory( "subdir/the-destination" ).toPath();
        try
        {
            new Loader().load( archive, destination, destination );
            fail( "Expected an exception" );
        }
        catch ( NoSuchFileException e )
        {
            assertEquals( destination.getParent().toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheTxLogsParentDirectoryDoesntExist()
            throws IOException, IncorrectFormat
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.file( "destination" ).toPath();
        Path txLogsDestination = testDirectory.directory( "subdir/txLogs" ).toPath();
        try
        {
            new Loader().load( archive, destination, txLogsDestination );
            fail( "Expected an exception" );
        }
        catch ( NoSuchFileException e )
        {
            assertEquals( txLogsDestination.getParent().toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheDestinationsParentDirectoryIsAFile()
            throws IOException, IncorrectFormat
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.directory( "subdir/the-destination" ).toPath();
        Files.write( destination.getParent(), new byte[0] );
        try
        {
            new Loader().load( archive, destination, destination );
            fail( "Expected an exception" );
        }
        catch ( FileSystemException e )
        {
            assertEquals( destination.getParent().toString() + ": Not a directory", e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheDestinationsParentDirectoryIsNotWritable()
            throws IOException, IncorrectFormat
    {
        assumeFalse( "We haven't found a way to reliably tests permissions on Windows", SystemUtils.IS_OS_WINDOWS );

        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.directory( "subdir/the-destination" ).toPath();
        Files.createDirectories( destination.getParent() );
        try ( Closeable ignored = withPermissions( destination.getParent(), emptySet() ) )
        {
            new Loader().load( archive, destination, destination );
            fail( "Expected an exception" );
        }
        catch ( AccessDeniedException e )
        {
            assertEquals( destination.getParent().toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheTxLogsParentDirectoryIsNotWritable()
            throws IOException, IncorrectFormat
    {
        assumeFalse( "We haven't found a way to reliably tests permissions on Windows", SystemUtils.IS_OS_WINDOWS );

        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Path destination = testDirectory.file( "destination" ).toPath();
        Path txLogsDrectory = testDirectory.directory( "subdir/txLogs" ).toPath();
        Files.createDirectories( txLogsDrectory.getParent() );
        try ( Closeable ignored = withPermissions( txLogsDrectory.getParent(), emptySet() ) )
        {
            new Loader().load( archive, destination, txLogsDrectory );
            fail( "Expected an exception" );
        }
        catch ( AccessDeniedException e )
        {
            assertEquals( txLogsDrectory.getParent().toString(), e.getMessage() );
        }
    }
}
