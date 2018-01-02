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
package org.neo4j.collection.primitive;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class PrimitiveLongIntKeyValueArrayTest
{
    private static final int DEFAULT_VALUE = -1;

    @Test
    public void testEnsureCapacity() throws Exception
    {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        assertThat( map.capacity(), equalTo( PrimitiveLongIntKeyValueArray.DEFAULT_INITIAL_CAPACITY
        ) );

        map.ensureCapacity( 10 );
        assertThat( map.capacity(), greaterThanOrEqualTo( 10 ) );

        map.ensureCapacity( 100 );
        assertThat( map.capacity(), greaterThanOrEqualTo( 100 ) );

        map.ensureCapacity( 1000 );
        assertThat( map.capacity(), greaterThanOrEqualTo( 1000 ) );

    }

    @Test
    public void testSize() throws Exception
    {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        assertThat( map.size(), equalTo( 0 ) );

        map.putIfAbsent( 1, 100 );
        map.putIfAbsent( 2, 200 );
        map.putIfAbsent( 3, 300 );
        assertThat( map.size(), equalTo( 3 ) );

    }

    @Test
    public void testGetOrDefault() throws Exception
    {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        map.putIfAbsent( 1, 100 );
        map.putIfAbsent( 2, 200 );
        map.putIfAbsent( 3, 300 );

        assertThat( map.getOrDefault( 1, DEFAULT_VALUE ), equalTo( 100 ) );
        assertThat( map.getOrDefault( 2, DEFAULT_VALUE ), equalTo( 200 ) );
        assertThat( map.getOrDefault( 3, DEFAULT_VALUE ), equalTo( 300 ) );
        assertThat( map.getOrDefault( 4, DEFAULT_VALUE ), equalTo( DEFAULT_VALUE ) );

    }

    @Test
    public void testPutIfAbsent() throws Exception
    {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();

        assertThat( map.putIfAbsent( 1, 100 ), equalTo( true ) );
        assertThat( map.putIfAbsent( 2, 200 ), equalTo( true ) );
        assertThat( map.putIfAbsent( 3, 300 ), equalTo( true ) );
        assertThat( map.size(), equalTo( 3 ) );
        assertThat( map.keys(), equalTo( new long[]{1, 2, 3} ) );

        assertThat( map.putIfAbsent( 2, 2000 ), equalTo( false ) );
        assertThat( map.putIfAbsent( 3, 3000 ), equalTo( false ) );
        assertThat( map.putIfAbsent( 4, 4000 ), equalTo( true ) );
        assertThat( map.size(), equalTo( 4 ) );
        assertThat( map.keys(), equalTo( new long[]{1, 2, 3, 4} ) );
        assertThat( map.getOrDefault( 2, DEFAULT_VALUE ), equalTo( 200 ) );
        assertThat( map.getOrDefault( 3, DEFAULT_VALUE ), equalTo( 300 ) );
        assertThat( map.getOrDefault( 4, DEFAULT_VALUE ), equalTo( 4000 ) );

    }

    @Test
    public void testReset() throws Exception
    {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        map.putIfAbsent( 1, 100 );
        map.putIfAbsent( 2, 200 );
        map.putIfAbsent( 3, 300 );

        map.reset( 1000 );
        assertThat( map.size(), equalTo( 0 ) );
        assertThat( map.capacity(), greaterThanOrEqualTo( 1000 ) );

    }

    @Test
    public void testKeys() throws Exception
    {
        PrimitiveLongIntKeyValueArray map = new PrimitiveLongIntKeyValueArray();
        map.putIfAbsent( 1, 100 );
        map.putIfAbsent( 2, 200 );
        map.putIfAbsent( 3, 300 );
        map.putIfAbsent( 2, 200 );
        map.putIfAbsent( 3, 300 );
        map.putIfAbsent( 8, 800 );
        map.putIfAbsent( 7, 700 );
        map.putIfAbsent( 6, 600 );
        map.putIfAbsent( 5, 500 );

        assertThat( map.size(), equalTo( 7 ) );
        assertThat( map.keys(), equalTo( new long[]{1, 2, 3, 8, 7, 6, 5} ) );

    }

}