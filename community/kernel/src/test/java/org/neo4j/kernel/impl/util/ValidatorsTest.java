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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertThrows;

@TestDirectoryExtension
class ValidatorsTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldFindFilesByRegex() throws Exception
    {
        // GIVEN
        existenceOfFile( "abc" );
        existenceOfFile( "bcd" );

        // WHEN/THEN
        assertValid( "abc" );
        assertValid( "bcd" );
        assertValid( "ab." );
        assertValid( ".*bc" );
        assertNotValid( "abcd" );
        assertNotValid( ".*de.*" );
    }

    private void assertNotValid( String string )
    {
        assertThrows( IllegalArgumentException.class, () -> validate( string ) );
    }

    private void assertValid( String fileByName )
    {
        validate( fileByName );
    }

    private void validate( String fileByName )
    {
        Validators.REGEX_FILE_EXISTS.validate( directory.file( fileByName ) );
    }

    private void existenceOfFile( String name ) throws IOException
    {
        directory.file( name ).createNewFile();
    }
}
