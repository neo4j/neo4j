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
package org.neo4j.kernel.database;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class DatabaseNameValidatorTest
{
    @Test
    void shouldNotGetAnErrorForAValidDatabaseName()
    {
        assertValid( "my.Vaild-Db123" );
    }

    @Test
    void shouldGetAnErrorForAnEmptyDatabaseName()
    {
        try
        {
            assertValid( "" );
            fail( "Expected exception \"The provided database name is empty.\" but succeeded." );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "The provided database name is empty.", e.getMessage() );
        }

        try
        {
            DatabaseNameValidator.assertValidDatabaseName( null );
            fail( "Expected exception \"The provided database name is empty.\" but succeeded." );
        }
        catch ( NullPointerException e )
        {
            assertEquals( "The provided database name is empty.", e.getMessage() );
        }
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidCharacters()
    {
        try
        {
            assertValid( "database%" );

            fail( "Expected exception \"Database name 'database%' contains illegal characters.\" but succeeded." );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Database name 'database%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.", e.getMessage() );
        }

        try
        {
            assertValid( "data_base" );

            fail( "Expected exception \"Database name 'data_base' contains illegal characters.\" but succeeded." );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Database name 'data_base' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.", e.getMessage() );
        }

        try
        {
            assertValid( "dataåäö" );

            fail( "Expected exception \"Database name 'dataåäö' contains illegal characters.\" but succeeded." );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Database name 'dataåäö' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.", e.getMessage() );
        }
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidFirstCharacter()
    {
        try
        {
            assertValid( "3database" );

            fail( "Expected exception \"Database name '3database' is not starting with an ASCII alphabetic character.\" but succeeded." );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Database name '3database' is not starting with an ASCII alphabetic character.", e.getMessage() );
        }
        try
        {
            assertValid( "_database" );

            fail( "Expected exception \"Database name '_database' is not starting with an ASCII alphabetic character.\" but succeeded." );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Database name '_database' is not starting with an ASCII alphabetic character.", e.getMessage() );
        }
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithSystemPrefix()
    {
        try
        {
            assertValid( "systemdatabase" );

            fail( "Expected exception \"Database name 'systemdatabase' is invalid, due to the prefix 'system'.\" but succeeded." );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Database name 'systemdatabase' is invalid, due to the prefix 'system'.", e.getMessage() );
        }
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidLength()
    {
        try
        {
            // Too short
            assertValid( "me" );

            fail( "Expected exception \"The provided database name must have a length between 3 and 63 characters.\" but succeeded." );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "The provided database name must have a length between 3 and 63 characters.", e.getMessage() );
        }

        try
        {
            // Too long
            assertValid( "ihaveallooootoflettersclearlymorethenishould-ihaveallooootoflettersclearlymorethenishould" );

            fail( "Expected exception \"The provided database name must have a length between 3 and 63 characters.\" but succeeded." );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "The provided database name must have a length between 3 and 63 characters.", e.getMessage() );
        }
    }

    private void assertValid( String name )
    {
        DatabaseNameValidator.assertValidDatabaseName( new NormalizedDatabaseName( name ) );
    }
}
