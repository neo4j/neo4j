/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.config.portable;

import org.junit.Test;

import org.neo4j.helpers.Function;

import static org.junit.Assert.assertEquals;

public class VariableSubstitutorTest
{
    private static final Function<String, String> TO_UPPER_CASE = new Function<String, String>()
    {
        @Override
        public String apply( String s )
        {
            return s.toUpperCase();
        }
    };

    private final VariableSubstitutor substitutor = new VariableSubstitutor( );

    @Test
    public void shouldAcceptEmptyInput()
    {
        assertEquals( "", substitutor.substitute( "", null ) );
    }


    @Test
    public void shouldAcceptInputWithoutVariables()
    {
        String expected = "Hello/Kitty/{TEST}";
        assertEquals( expected, substitutor.substitute( expected, null ) );
    }


    @Test
    public void shouldSubstituteVariable()
    {
        assertEquals( "TEST", substitutor.substitute( "${test}", TO_UPPER_CASE ) );
    }

    @Test
    public void shouldSubstituteMultipleVariables()
    {
        assertEquals( "TESTTEXT", substitutor.substitute( "${test}${text}", TO_UPPER_CASE ) );
    }

    @Test
    public void shouldSubstituteMultipleVariablesInText()
    {
        assertEquals(
            "APPDATA/neo4j-desktop.vmoptions",
                substitutor.substitute( "${APPDATA}/neo4j-desktop.vmoptions", TO_UPPER_CASE ) );
    }

    @Test
    public void shouldSubstituteMultipleVariablesInMiddleOfText()
    {
        assertEquals( "do/TEST/and/VERIFY", substitutor.substitute( "do/${test}/and/${verify}", TO_UPPER_CASE ) );
    }
}
