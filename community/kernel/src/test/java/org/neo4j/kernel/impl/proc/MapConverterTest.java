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
package org.neo4j.kernel.impl.proc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntMap;

public class MapConverterTest
{
    private final MapConverter converter = new MapConverter();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldHandleNullString()
    {
        // Given
        String mapString = "null";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( null ) ) );
    }

    @Test
    public void shouldHandleEmptyMap()
    {
        // Given
        String mapString = "{}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( emptyMap() ) ) );
    }

    @Test
    public void shouldHandleEmptyMapWithSpaces()
    {
        // Given
        String mapString = " {  }  ";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( emptyMap() ) ) );
    }

    @Test
    public void shouldHandleSingleQuotedValue()
    {
        // Given
        String mapString = "{key: 'value'}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "value" ) ) ) );
    }

    @Test
    public void shouldHandleEscapedSingleQuotedInValue1()
    {
        // Given
        String mapString = "{key: 'va\'lue'}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "va\'lue" ) ) ) );
    }

    @Test
    public void shouldHandleEscapedSingleQuotedInValue2()
    {
        // Given
        String mapString = "{key: \"va\'lue\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "va\'lue" ) ) ) );
    }

    @Test
    public void shouldHandleEscapedDoubleQuotedInValue1()
    {
        // Given
        String mapString = "{key: \"va\"lue\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "va\"lue" ) ) ) );
    }

    @Test
    public void shouldHandleEscapedDoubleQuotedInValue2()
    {
        // Given
        String mapString = "{key: 'va\"lue'}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "va\"lue" ) ) ) );
    }

    @Test
    public void shouldHandleDoubleQuotedValue()
    {
        // Given
        String mapString = "{key: \"value\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "value" ) ) ) );
    }

    @Test
    public void shouldHandleSingleQuotedKey()
    {
        // Given
        String mapString = "{'key;: 'value'}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "value" ) ) ) );
    }

    @Test
    public void shouldHandleDoubleQuotedKey()
    {
        // Given
        String mapString = "{\"key\": \"value\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "value" ) ) ) );
    }

    @Test
    public void shouldHandleKeyWithEscapedSingleQuote()
    {
        // Given
        String mapString = "{\"k\'ey\": \"value\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "k\'ey", "value" ) ) ) );
    }

    @Test
    public void shouldHandleKeyWithEscapedDoubleQuote()
    {
        // Given
        String mapString = "{\"k\"ey\": \"value\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "k\"ey", "value" ) ) ) );
    }

    @Test
    public void shouldHandleIntegerValue()
    {
        // Given
        String mapString = "{key: 1337}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", 1337L ) ) ) );
    }

    @Test
    public void shouldHandleFloatValue()
    {
        // Given
        String mapString = "{key: 2.718281828}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", 2.718281828 ) ) ) );
    }

    @Test
    public void shouldHandleNullValue()
    {
        // Given
        String mapString = "{key: null}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", null ) ) ) );
    }

    @Test
    public void shouldHandleFalseValue()
    {
        // Given
        String mapString = "{key: false}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", false ) ) ) );
    }

    @Test
    public void shouldHandleTrueValue()
    {
        // Given
        String mapString = "{key: true}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", true ) ) ) );
    }

    @Test
    public void shouldHandleMultipleKeys()
    {
        // Given
        String mapString = "{k1: 2.718281828, k2: 'e'}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "k1", 2.718281828, "k2", "e" ) ) ) );
    }

    @Test
    public void shouldFailWhenDuplicateKey()
    {
        // Given
        String mapString = "{k1: 2.718281828, k1: 'e'}";

        // Expect
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Multiple occurrences of key 'k1'" );

        // When
        converter.apply( mapString );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldHandleNestedMaps()
    {
        // Given
        String mapString = "{k1: 1337, k2: { k1 : 1337, k2: {k1: 1337}}}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        Map<String,Object> map1 = (Map<String,Object>) converted.value();
        Map<String,Object> map2 = (Map<String,Object>) map1.get( "k2" );
        Map<String,Object> map3 = (Map<String,Object>) map2.get( "k2" );
        assertThat( map1.get( "k1" ), equalTo( 1337L ) );
        assertThat( map2.get( "k1" ), equalTo( 1337L ) );
        assertThat( map3.get( "k1" ), equalTo( 1337L ) );
    }

    @Test
    public void shouldFailOnMalformedMap()
    {
        // Given
        String mapString = "{k1: 2.718281828, k2: 'e'}}";

        // Expect
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "{k1: 2.718281828, k2: 'e'}} contains unbalanced '{', '}'." );

        // When
        converter.apply( mapString );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldHandleMapsWithLists()
    {
        // Given
        String mapString = "{k1: [1337, 42]}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        Map<String,Object> map1 = (Map<String,Object>) converted.value();
        assertThat( map1.get( "k1" ), equalTo( asList( 1337L, 42L ) ) );

    }
}
