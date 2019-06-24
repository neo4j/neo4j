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
package org.neo4j.configuration.helpers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        Exception e = assertThrows( IllegalArgumentException.class, () -> assertValid( "" ) );
        assertEquals( "The provided database name is empty.", e.getMessage() );

        Exception e2 = assertThrows( NullPointerException.class, () -> DatabaseNameValidator.assertValidDatabaseName( null ) );
        assertEquals( "The provided database name is empty.", e2.getMessage() );
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidCharacters()
    {
        Exception e = assertThrows( IllegalArgumentException.class, () ->  assertValid( "database%" ) );
        assertEquals( "Database name 'database%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.", e.getMessage() );

        Exception e2 = assertThrows( IllegalArgumentException.class, () ->  assertValid( "data_base" ) );
        assertEquals( "Database name 'data_base' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.", e2.getMessage() );

        Exception e3 = assertThrows( IllegalArgumentException.class, () ->  assertValid( "dataåäö" ) );
        assertEquals( "Database name 'dataåäö' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.", e3.getMessage() );
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidFirstCharacter()
    {
        Exception e = assertThrows( IllegalArgumentException.class, () ->  assertValid( "3database" ) );
        assertEquals( "Database name '3database' is not starting with an ASCII alphabetic character.", e.getMessage() );

        Exception e2 = assertThrows( IllegalArgumentException.class, () ->  assertValid( "_database" ) );
        assertEquals( "Database name '_database' is not starting with an ASCII alphabetic character.", e2.getMessage() );
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithSystemPrefix()
    {
        Exception e = assertThrows( IllegalArgumentException.class, () ->  assertValid( "systemdatabase" ) );
        assertEquals( "Database name 'systemdatabase' is invalid, due to the prefix 'system'.", e.getMessage() );
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidLength()
    {
        // Too short
        Exception e = assertThrows( IllegalArgumentException.class, () ->  assertValid( "me" ) );
        assertEquals( "The provided database name must have a length between 3 and 63 characters.", e.getMessage() );

        // Too long
        Exception e2 = assertThrows( IllegalArgumentException.class,
                () -> assertValid( "ihaveallooootoflettersclearlymorethenishould-ihaveallooootoflettersclearlymorethenishould" ) );
        assertEquals( "The provided database name must have a length between 3 and 63 characters.", e2.getMessage() );
    }

    private void assertValid( String name )
    {
        DatabaseNameValidator.assertValidDatabaseName( new NormalizedDatabaseName( name ) );
    }
}
