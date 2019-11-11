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

class GraphNameValidatorTest
{
    @Test
    void shouldNotGetAnErrorForAValidGraphName()
    {
        checkGraphName( "my.Vaild-Db123" );
    }

    @Test
    void shouldGetAnErrorForAnEmptyGraphName()
    {
        Exception e = assertThrows( IllegalArgumentException.class, () -> checkGraphName( "" ) );
        assertEquals( "The provided graph name is empty.", e.getMessage() );

        Exception e2 = assertThrows( NullPointerException.class, () -> GraphNameValidator.assertValidGraphName( null ) );
        assertEquals( "The provided graph name is empty.", e2.getMessage() );
    }

    @Test
    void shouldGetAnErrorForAGraphNameWithInvalidCharacters()
    {
        Exception e = assertThrows( IllegalArgumentException.class, () -> checkGraphName( "graph%" ) );
        assertEquals( "Graph name 'graph%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.", e.getMessage() );

        Exception e2 = assertThrows( IllegalArgumentException.class, () -> checkGraphName( "data_base" ) );
        assertEquals( "Graph name 'data_base' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.", e2.getMessage() );

        Exception e3 = assertThrows( IllegalArgumentException.class, () -> checkGraphName( "dataåäö" ) );
        assertEquals( "Graph name 'dataåäö' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.", e3.getMessage() );
    }

    @Test
    void shouldGetAnErrorForAGraphNameWithInvalidFirstCharacter()
    {
        Exception e = assertThrows( IllegalArgumentException.class, () -> checkGraphName( "3graph" ) );
        assertEquals( "Graph name '3graph' is not starting with an ASCII alphabetic character.", e.getMessage() );

        Exception e2 = assertThrows( IllegalArgumentException.class, () -> checkGraphName( "_graph" ) );
        assertEquals( "Graph name '_graph' is not starting with an ASCII alphabetic character.", e2.getMessage() );
    }

    @Test
    void shouldGetAnErrorForAGraphNamedGraph()
    {
        Exception e = assertThrows( IllegalArgumentException.class, () -> checkGraphName( "graph" ) );
        assertEquals( "Graph name 'graph' is reserved.", e.getMessage() );
    }

    @Test
    void shouldGetAnErrorForAGraphNameWithInvalidLength()
    {
        // Too short
        Exception e = assertThrows( IllegalArgumentException.class, () -> checkGraphName( "me" ) );
        assertEquals( "The provided graph name must have a length between 3 and 63 characters.", e.getMessage() );

        // Too long
        Exception e2 = assertThrows( IllegalArgumentException.class,
                                     () -> checkGraphName( "ihaveallooootoflettersclearlymorethenishould-ihaveallooootoflettersclearlymorethenishould" ) );
        assertEquals( "The provided graph name must have a length between 3 and 63 characters.", e2.getMessage() );
    }

    private void checkGraphName( String name )
    {
        GraphNameValidator.assertValidGraphName( new NormalizedGraphName( name ) );
    }
}
