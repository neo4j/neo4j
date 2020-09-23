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
package org.neo4j.kernel.impl.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.neo4j.kernel.impl.util.Converters.regexFiles;

@TestDirectoryExtension
class ConvertersTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldSortFilesByNumberCleverly() throws Exception
    {
        // GIVEN
        Path file1 = existenceOfFile( "file1" );
        Path file123 = existenceOfFile( "file123" );
        Path file12 = existenceOfFile( "file12" );
        Path file2 = existenceOfFile( "file2" );
        Path file32 = existenceOfFile( "file32" );

        // WHEN
        Path[] files = regexFiles( true ).apply( directory.file( "file" ).toAbsolutePath().toString() + ".*" );

        // THEN
        assertArrayEquals( new Path[]{file1, file2, file12, file32, file123}, files );
    }

    private Path existenceOfFile( String name ) throws IOException
    {
        Path file = directory.file( name );
        Files.createFile( file );
        return file;
    }
}
