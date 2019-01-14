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
package org.neo4j.values.virtual;

import org.junit.Test;

import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.map;

public class MapTest
{
    @Test
    public void shouldBeEqualToItself()
    {
        assertEqual(
                map( "1", false, "20", new short[]{4} ),
                map( "1", false, "20", new short[]{4} ) );

        assertEqual(
                map( "1", 101L, "20", "yo" ),
                map( "1", 101L, "20", "yo" ) );
    }

    @Test
    public void shouldCoerce()
    {
        assertEqual(
                map( "1", 1, "20", 'a' ),
                map( "1", 1.0, "20", "a" ) );

        assertEqual(
                map( "1", new byte[]{1}, "20", new String[]{"x"} ),
                map( "1", new short[]{1}, "20", new char[]{'x'} ) );

        assertEqual(
                map( "1", new int[]{1}, "20", new double[]{2.0} ),
                map( "1", new float[]{1.0f}, "20", new float[]{2.0f} ) );
    }

    @Test
    public void shouldRecurse()
    {
        assertEqual(
                map( "1", map( "2", map( "3", "hi" ) ) ),
                map( "1", map( "2", map( "3", "hi" ) ) ) );
    }

    @Test
    public void shouldRecurseAndCoerce()
    {
        assertEqual(
                map( "1", map( "2", map( "3", "x" ) ) ),
                map( "1", map( "2", map( "3", 'x' ) ) ) );

        assertEqual(
                map( "1", map( "2", map( "3", 1.0 ) ) ),
                map( "1", map( "2", map( "3", 1 ) ) ) );
    }
}
