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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.values.AnyValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.map;

class MapValueTest
{
    @Test
    void shouldFilterOnKeys()
    {
        // Given
        MapValue base = mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ), "k3", stringValue( "v3" ) );

        // When
        MapValue filtered = base.filter( ( k, ignore ) -> k.equals( "k2" ) );

        // Then
        assertMapValueEquals( filtered, mapValue( "k2", stringValue( "v2" ) ) );
    }

    @Test
    void shouldFilterOnValues()
    {
        // Given
        MapValue base = mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ), "k3", stringValue( "v3" ) );

        // When
        MapValue filtered = base.filter( ( ignore, v ) -> v.equals( stringValue( "v2" ) ) );

        // Then
        assertMapValueEquals( filtered, mapValue( "k2", stringValue( "v2" ) ) );
    }

    @Test
    void shouldFilterOnKeysAndValues()
    {
        // Given
        MapValue base = mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ), "k3", stringValue( "v3" ) );

        // When
        MapValue filtered = base.filter( ( k, v ) -> k.equals( "k1" ) && v.equals( stringValue( "v2" ) ) );

        // Then
        assertMapValueEquals( filtered, EMPTY_MAP );

    }

    @Test
    void shouldUpdateWithIdenticalValues()
    {
        // Given
        MapValue base = mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ), "k3", stringValue( "v3" ) );

        // When
        MapValue updated = base.updatedWith( "k3", stringValue( "v3" ) );

        // Then
        assertMapValueEquals( updated, base );
    }

    @Test
    void shouldUpdateWithExistingKey()
    {
        // Given
        MapValue base = mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ), "k3", stringValue( "v3" ) );

        // When
        MapValue updated = base.updatedWith( "k3", stringValue( "version3" ) );

        // Then
        assertMapValueEquals( updated, mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ),
                "k3", stringValue( "version3" ) ) );
    }

    @Test
    void shouldUpdateWithNewKey()
    {
        // Given
        MapValue base = mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ), "k3", stringValue( "v3" ) );

        // When
        MapValue updated = base.updatedWith( "k4", stringValue( "v4" ) );

        // Then
        assertMapValueEquals( updated, mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ),
                "k3", stringValue( "v3" ), "k4", stringValue( "v4" ) ) );
    }

    @Test
    void shouldUpdateWithOtherMapValue()
    {
        // Given
        MapValue a = mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ), "k3", stringValue( "v3" ) );
        MapValue b = mapValue( "k1", stringValue( "version1" ), "k2", stringValue( "version2" ),
                "k4", stringValue( "version4" ) );

        // When
        MapValue updated = a.updatedWith( b );

        // Then
        assertMapValueEquals( updated, mapValue(
                "k1", stringValue( "version1" ),
                "k2", stringValue( "version2" ),
                "k3", stringValue( "v3" ),
                "k4", stringValue( "version4" )
        ) );
    }

    @Test
    void shouldUpdateMultipleTimesMapValue()
    {
        // Given
        MapValue a = mapValue( "k1", stringValue( "v1" ), "k2", stringValue( "v2" ) );
        MapValue b = mapValue( "k1", stringValue( "version1" ), "k4", stringValue( "version4" ) );
        MapValue c = mapValue( "k3", stringValue( "v3" ) );

        // When
        MapValue updated = a.updatedWith( b ).updatedWith( c );

        // Then
        assertMapValueEquals( updated, mapValue(
                "k1", stringValue( "version1" ),
                "k2", stringValue( "v2" ),
                "k3", stringValue( "v3" ),
                "k4", stringValue( "version4" )
        ) );
    }

    private void assertMapValueEquals( MapValue a, MapValue b )
    {
        assertThat( a, equalTo( b ) );
        assertThat( a.size(), equalTo( b.size() ) );
        assertThat( a.hashCode(), equalTo( b.hashCode() ) );
        assertThat( a.keySet(), containsInAnyOrder( Iterables.asArray( String.class, b.keySet() ) ) );
        assertThat( Arrays.asList( a.keys().asArray() ), containsInAnyOrder( b.keys().asArray() ) );
        a.foreach( ( k, v ) -> assertThat( b.get( k ), equalTo( v ) ) );
        b.foreach( ( k, v ) -> assertThat( a.get( k ), equalTo( v ) ) );
    }

    private MapValue mapValue( Object... kv )
    {
        assert kv.length % 2 == 0;
        String[] keys = new String[kv.length / 2];
        AnyValue[] values = new AnyValue[kv.length / 2];
        for ( int i = 0; i < kv.length; i += 2 )
        {
            keys[i / 2] = (String) kv[i];
            values[i / 2] = (AnyValue) kv[i + 1];
        }
        return map( keys, values );
    }
}
