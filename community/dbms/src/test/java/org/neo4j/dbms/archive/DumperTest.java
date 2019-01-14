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

import org.neo4j.function.Predicates;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class DumperTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldGiveAClearErrorIfTheArchiveAlreadyExists() throws IOException
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Files.write( archive, new byte[0] );
        try
        {
            new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() );
            fail( "Expected an exception" );
        }
        catch ( FileAlreadyExistsException e )
        {
            assertEquals( archive.toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheDirectoryDoesntExist() throws IOException
    {
        Path directory = testDirectory.file( "a-directory" ).toPath();
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        try
        {
            new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() );
            fail( "Expected an exception" );
        }
        catch ( NoSuchFileException e )
        {
            assertEquals( directory.toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryDoesntExist() throws IOException
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path archive = testDirectory.file( "subdir/the-archive.dump" ).toPath();
        try
        {
            new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() );
            fail( "Expected an exception" );
        }
        catch ( NoSuchFileException e )
        {
            assertEquals( archive.getParent().toString(), e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryIsAFile() throws IOException
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path archive = testDirectory.file( "subdir/the-archive.dump" ).toPath();
        Files.write( archive.getParent(), new byte[0] );
        try
        {
            new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() );
            fail( "Expected an exception" );
        }
        catch ( FileSystemException e )
        {
            assertEquals( archive.getParent().toString() + ": Not a directory", e.getMessage() );
        }
    }

    @Test
    public void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryIsNotWritable() throws IOException
    {
        assumeFalse( "We haven't found a way to reliably tests permissions on Windows", SystemUtils.IS_OS_WINDOWS );

        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path archive = testDirectory.file( "subdir/the-archive.dump" ).toPath();
        Files.createDirectories( archive.getParent() );
        try ( Closeable ignored = TestUtils.withPermissions( archive.getParent(), emptySet() ) )
        {
            new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() );
            fail( "Expected an exception" );
        }
        catch ( AccessDeniedException e )
        {
            assertEquals( archive.getParent().toString(), e.getMessage() );
        }
    }
}
