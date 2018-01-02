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
package org.neo4j.helpers;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.Strings.decamelize;
import static org.neo4j.helpers.Strings.prettyPrint;

public class StringsTest
{
    @Test
    public void testDecamelize()
    {
        assertEquals( "foo", decamelize.apply( "foo" ) );
        assertEquals( "foo", decamelize.apply( "Foo" ) );
        assertEquals( "foo_bar", decamelize.apply( "FooBar" ) );
        assertEquals( "f_b", decamelize.apply( "FB" ) );
        assertEquals( "_", decamelize.apply( "_" ) );
        // What is expected behaviour here?
//        assertEquals( "f_b", decamelize.apply( "F_B" ) );
    }

    @Test
    public void testPrettyPrint()
    {
        assertEquals( "null", prettyPrint( null ) );
        assertEquals( "42", prettyPrint( 42 ) );
        assertEquals( "42", prettyPrint( "42" ) );
        assertEquals( "[1, 2, 3, 4]", prettyPrint( new int[]{1, 2, 3, 4} ) );
        assertEquals( "[false, true, true, false]", prettyPrint( new boolean[]{false, true, true, false} ) );
        assertEquals( "[a, b, z]", prettyPrint( new char[]{'a', 'b', 'z'} ) );
        assertEquals( "[ab, cd, zx]", prettyPrint( new String[]{"ab", "cd", "zx"} ) );
        assertEquals(
                "[Cat, [http://neo4j.com, http://neo4j.org], Dog, [1, 2, 3], [[[Wolf]]]]",
                prettyPrint( new Object[]{
                        "Cat",
                        new URI[]{URI.create( "http://neo4j.com" ), URI.create( "http://neo4j.org" )},
                        "Dog",
                        new int[]{1, 2, 3},
                        new Object[]{new Object[]{new Object[]{"Wolf"}}}} ) );

        Object[] recursiveArray = {10.12345, null, "String"};
        recursiveArray[1] = recursiveArray;
        assertEquals( "[10.12345, [...], String]", prettyPrint( recursiveArray ) );
    }

    @Test
    public void testEscape()
    {
        assertEquals( "abc", Strings.escape( "abc" ) );
        assertEquals( "Abc", Strings.escape( "Abc" ) );
        assertEquals( "a\\\"bc", Strings.escape( "a\"bc" ) );
        assertEquals( "a\\\'bc", Strings.escape( "a\'bc" ) );
        assertEquals( "a\\\\bc", Strings.escape( "a\\bc" ) );
        assertEquals( "a\\nbc", Strings.escape( "a\nbc" ) );
        assertEquals( "a\\tbc", Strings.escape( "a\tbc" ) );
        assertEquals( "a\\rbc", Strings.escape( "a\rbc" ) );
        assertEquals( "a\\bbc", Strings.escape( "a\bbc" ) );
        assertEquals( "a\\fbc", Strings.escape( "a\fbc" ) );
    }

    @Test
    public void testJoiningLines()
    {
        assertEquals( "a" + System.lineSeparator() + "b" + System.lineSeparator() + "c" + System.lineSeparator(),
                Strings.joinAsLines( "a", "b", "c" ) );
    }
}
