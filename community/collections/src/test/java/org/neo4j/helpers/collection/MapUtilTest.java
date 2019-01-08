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
package org.neo4j.helpers.collection;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MapUtilTest
{
    @Test
    public void loadSpacedValue() throws Exception
    {
        // expecting
        Map<String,String> expected = new HashMap<>();
        expected.put( "key", "value" );

        // given
        InputStream inputStream = new ByteArrayInputStream( "   key   =   value   ".getBytes() );

        // when
        Map<String,String> result = MapUtil.load( inputStream );

        // then
        assertEquals( expected, result );
    }

    @Test
    public void loadNothing() throws Exception
    {
        // expecting
        Map<String,String> expected = new HashMap<>();
        expected.put( "key", "" );

        // given
        InputStream inputStream = new ByteArrayInputStream( "   key   =      ".getBytes() );

        // when
        Map<String,String> result = MapUtil.load( inputStream );

        // then
        assertEquals( expected, result );
    }
}
