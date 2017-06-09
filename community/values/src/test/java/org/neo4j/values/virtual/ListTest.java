/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.virtual;

import org.junit.Test;

import org.neo4j.values.AnyValue;
import org.neo4j.values.Values;
import org.neo4j.values.VirtualValue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertNotEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.list;

public class ListTest
{
    @Test
    public void shouldBeEqualToItself()
    {
        assertEqual(
                list( new String[]{"hi"}, 3.0 ),
                list( new String[]{"hi"}, 3.0 ) );

        assertEqual( list(), list() );
    }

    @Test
    public void shouldNotEqual()
    {
        assertNotEqual( list(), list( 2 ) );
        assertNotEqual( list(), list( 1, 2 ) );
        assertNotEqual( list( 1 ), list( 2 ) );
        assertNotEqual( list( 1 ), list( 1, 2 ) );
        assertNotEqual( list( 1, 1 ), list( 1, 2 ) );
        assertNotEqual( list( 1, "d" ), list( 1, "f" ) );
        assertNotEqual( list( 1, "d" ), list( "d", 1 ) );
        assertNotEqual( list( "d" ), list( false ) );
        assertNotEqual(
                list( Values.stringArray( new String[]{"d"} ) ),
                list( "d" ) );

        assertNotEqual(
                list( Values.longArray( new long[]{3, 4, 5} ) ),
                list( Values.intArray( new int[]{3, 4, 50} ) ) );
    }

    @Test
    public void shouldCoerce()
    {
        assertEqual(
                list( new String[]{"h"}, 3.0 ),
                list( new char[]{'h'}, 3 ) );
    }

    @Test
    public void shouldRecurse()
    {
        assertEqual(
                list( 'a', list( 'b', list( 'c' ) ) ),
                list( 'a', list( 'b', list( 'c' ) ) ) );
    }

    @Test
    public void shouldRecurseAndCoerce()
    {
        assertEqual(
                list( "a", list( 'b', list( "c" ) ) ),
                list( 'a', list( "b", list( 'c' ) ) ) );
    }
}
