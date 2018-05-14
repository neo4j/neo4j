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
package org.neo4j.values.virtual;

import org.junit.jupiter.api.Test;

import org.neo4j.values.AnyValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.values.storable.Values.stringValue;

class MapValueTest
{
    @Test
    void shouldFilterOnKeys()
    {
        // Given
        MapValue base = VirtualValues.map( new String[]{"k1", "k2", "k3"},
                new AnyValue[]{stringValue( "v1" ), stringValue( "v2" ), stringValue( "v3" )} );

        // When
        MapValue filtered = base.filter( ( k, ignore ) -> k.equals( "k2" ) );

        // Then
        assertThat( filtered.size(), equalTo( 1 ) );
        assertThat( filtered.keys(), equalTo( VirtualValues.list( stringValue( "k2" ) ) ) );
        assertThat( filtered.get( "k2" ), equalTo( stringValue( "v2" ) ) );
    }

    @Test
    void shouldFilterOnValues()
    {
        // Given
        MapValue base = VirtualValues.map( new String[]{"k1", "k2", "k3"},
                new AnyValue[]{stringValue( "v1" ), stringValue( "v2" ), stringValue( "v3" )} );

        // When
        MapValue filtered = base.filter( ( ignore, v ) -> v.equals( stringValue( "v2" ) ) );

        // Then
        assertThat( filtered.size(), equalTo( 1 ) );
        assertThat( filtered.keys(), equalTo( VirtualValues.list( stringValue( "k2" ) ) ) );
        assertThat( filtered.get( "k2" ), equalTo( stringValue( "v2" ) ) );
    }

    @Test
    void shouldFilterOnKeysAndValues()
    {
        // Given
        MapValue base = VirtualValues.map( new String[]{"k1", "k2", "k3"},
                new AnyValue[]{stringValue( "v1" ), stringValue( "v2" ), stringValue( "v3" )} );

        // When
        MapValue filtered = base.filter( ( k, v ) -> k.equals( "k1" ) && v.equals( stringValue( "v2" ) ) );

        // Then
        assertThat( filtered.size(), equalTo( 0 ) );
    }

    @Test
    void shouldUpdateWithIdenticalValues()
    {
        // Given
        MapValue base = VirtualValues.map( new String[]{"k1", "k2", "k3"},
                new AnyValue[]{stringValue( "v1" ), stringValue( "v2" ), stringValue( "v3" )} );

        // When
        MapValue updated = base.updatedWith( "k3", stringValue( "v3" ) );

        // Then
        assertThat( updated, equalTo( base ) );
    }

    @Test
    void shouldUpdateWithExistingKey()
    {
        // Given
        MapValue base = VirtualValues.map( new String[]{"k1", "k2", "k3"},
                new AnyValue[]{stringValue( "v1" ), stringValue( "v2" ), stringValue( "v3" )} );

        // When
        MapValue updated = base.updatedWith( "k3", stringValue( "version3" ) );

        // Then
        assertThat( updated.size(), equalTo( 3 ) );
        assertThat( updated.get( "k1" ), equalTo( stringValue( "v1" ) ) );
        assertThat( updated.get( "k2" ), equalTo( stringValue( "v2" ) ) );
        assertThat( updated.get( "k3" ), equalTo( stringValue( "version3" ) ) );
    }

    @Test
    void shouldUpdateWithNewKey()
    {
        // Given
        MapValue base = VirtualValues.map( new String[]{"k1", "k2", "k3"},
                new AnyValue[]{stringValue( "v1" ), stringValue( "v2" ), stringValue( "v3" )} );

        // When
        MapValue updated = base.updatedWith( "k4", stringValue( "v4" ) );

        // Then
        assertThat( updated.size(), equalTo( 4 ) );
        assertThat( updated.get( "k1" ), equalTo( stringValue( "v1" ) ) );
        assertThat( updated.get( "k2" ), equalTo( stringValue( "v2" ) ) );
        assertThat( updated.get( "k3" ), equalTo( stringValue( "v3" ) ) );
        assertThat( updated.get( "k4" ), equalTo( stringValue( "v4" ) ) );
    }
}
