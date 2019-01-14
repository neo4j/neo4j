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

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ValidatorsTest
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

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

    @Test
    public void shouldValidateInList()
    {
        try
        {
            Validators.inList(new String[] { "foo", "bar", "baz" }).validate( "qux" );
            fail( "Should have failed to find item in list." );
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), containsString( "'qux' found but must be one of: [foo, bar, baz]." ) );
        }

        try
        {
            Validators.inList(new String[] { "foo", "bar", "baz" }).validate( "bar" );
        }
        catch ( IllegalArgumentException e )
        {
            fail( "Should have found item in list." );
        }
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
