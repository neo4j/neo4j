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
package org.neo4j.procedure.impl;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.cypher.internal.evaluator.Evaluator;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntMap;

class MapConverterTest
{
    private final MapConverter converter = new MapConverter( Evaluator.expressionEvaluator() );

    @Test
    void shouldHandleNullString()
    {
        // Given
        String mapString = "null";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( null ) ) );
    }

    @Test
    void shouldHandleEmptyMap()
    {
        // Given
        String mapString = "{}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( emptyMap() ) ) );
    }

    @Test
    void shouldHandleEmptyMapWithSpaces()
    {
        // Given
        String mapString = " {  }  ";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( emptyMap() ) ) );
    }

    @Test
    void shouldHandleSingleQuotedValue()
    {
        // Given
        String mapString = "{key: 'value'}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "value" ) ) ) );
    }

    @Test
    void shouldHandleEscapedSingleQuotedInValue2()
    {
        // Given
        String mapString = "{key: \"va\'lue\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "va\'lue" ) ) ) );
    }

    @Test
    void shouldHandleEscapedDoubleQuotedInValue1()
    {
        // Given
        String mapString = "{key: \"va\\\"lue\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "va\"lue" ) ) ) );
    }

    @Test
    void shouldHandleEscapedDoubleQuotedInValue2()
    {
        // Given
        String mapString = "{key: 'va\"lue'}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "va\"lue" ) ) ) );
    }

    @Test
    void shouldHandleDoubleQuotedValue()
    {
        // Given
        String mapString = "{key: \"value\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", "value" ) ) ) );
    }

    @Test
    void shouldHandleKeyWithEscapedSingleQuote()
    {
        // Given
        String mapString = "{`k\'ey`: \"value\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "k\'ey", "value" ) ) ) );
    }

    @Test
    void shouldHandleKeyWithEscapedDoubleQuote()
    {
        // Given
        String mapString = "{`k\"ey`: \"value\"}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "k\"ey", "value" ) ) ) );
    }

    @Test
    void shouldHandleIntegerValue()
    {
        // Given
        String mapString = "{key: 1337}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", 1337L ) ) ) );
    }

    @Test
    void shouldHandleFloatValue()
    {
        // Given
        String mapString = "{key: 2.718281828}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", 2.718281828 ) ) ) );
    }

    @Test
    void shouldHandleNullValue()
    {
        // Given
        String mapString = "{key: null}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", null ) ) ) );
    }

    @Test
    void shouldHandleFalseValue()
    {
        // Given
        String mapString = "{key: false}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", false ) ) ) );
    }

    @Test
    void shouldHandleTrueValue()
    {
        // Given
        String mapString = "{key: true}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "key", true ) ) ) );
    }

    @Test
    void shouldHandleMultipleKeys()
    {
        // Given
        String mapString = "{k1: 2.718281828, k2: 'e'}";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntMap( map( "k1", 2.718281828, "k2", "e" ) ) ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void shouldHandleNestedMaps()
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
    void shouldFailOnMalformedMap()
    {
        // Given
        String mapString = "{k1: 2.718281828, k2: 'e'}}";

        assertThrows( IllegalArgumentException.class, () -> converter.apply( mapString ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void shouldHandleMapsWithLists()
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
