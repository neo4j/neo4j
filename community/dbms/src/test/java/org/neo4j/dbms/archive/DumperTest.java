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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.neo4j.function.Predicates;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith( TestDirectoryExtension.class )
class DumperTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldGiveAClearErrorIfTheArchiveAlreadyExists() throws IOException
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        Files.write( archive, new byte[0] );
        FileAlreadyExistsException exception =
                assertThrows( FileAlreadyExistsException.class, () -> new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() ) );
        assertEquals( archive.toString(), exception.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheDirectoryDoesntExist()
    {
        Path directory = testDirectory.file( "a-directory" ).toPath();
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        NoSuchFileException exception =
                assertThrows( NoSuchFileException.class, () -> new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() ) );
        assertEquals( directory.toString(), exception.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryDoesntExist()
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path archive = testDirectory.file( "subdir/the-archive.dump" ).toPath();
        NoSuchFileException exception =
                assertThrows( NoSuchFileException.class, () -> new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() ) );
        assertEquals( archive.getParent().toString(), exception.getMessage() );
    }

    @Test
    void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryIsAFile() throws IOException
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path archive = testDirectory.file( "subdir/the-archive.dump" ).toPath();
        Files.write( archive.getParent(), new byte[0] );
        FileSystemException exception =
                assertThrows( FileSystemException.class, () -> new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() ) );
        assertEquals( archive.getParent().toString() + ": Not a directory", exception.getMessage() );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldGiveAClearErrorMessageIfTheArchivesParentDirectoryIsNotWritable() throws IOException
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path archive = testDirectory.file( "subdir/the-archive.dump" ).toPath();
        Files.createDirectories( archive.getParent() );
        try ( Closeable ignored = TestUtils.withPermissions( archive.getParent(), emptySet() ) )
        {
            AccessDeniedException exception =
                    assertThrows( AccessDeniedException.class, () -> new Dumper().dump( directory, directory, archive, Predicates.alwaysFalse() ) );
            assertEquals( archive.getParent().toString(), exception.getMessage() );
        }
    }
}
