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

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.fail;

public class ValidatorsTest
{
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldFindFilesByRegex() throws Exception
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
        try
        {
            validate( string );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {   // Good
        }
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
