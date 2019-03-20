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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.internal.evaluator.Evaluator;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.cypher.internal.evaluator.Evaluator.expressionEvaluator;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;

class ListConverterTest
{
    @Test
    void shouldHandleNullString()
    {
        // Given
        ListConverter converter = new ListConverter( String.class, NTString, expressionEvaluator() );
        String listString = "null";

        // When
        DefaultParameterValue converted = converter.apply( listString );

        // Then
        assertThat( converted, equalTo( ntList( null, NTString ) ) );
    }

    @Test
    void shouldHandleEmptyList()
    {
        // Given
        ListConverter converter = new ListConverter( String.class, NTString, expressionEvaluator() );
        String listString = "[]";

        // When
        DefaultParameterValue converted = converter.apply( listString );

        // Then
        assertThat( converted, equalTo( ntList( emptyList(), NTString ) ) );
    }

    @Test
    void shouldHandleEmptyListWithSpaces()
    {
        // Given
        ListConverter converter = new ListConverter( String.class, NTString, expressionEvaluator() );
        String listString = " [  ]   ";

        // When
        DefaultParameterValue converted = converter.apply( listString );

        // Then
        assertThat( converted, equalTo( ntList( emptyList(), NTString ) ) );
    }

    @Test
    void shouldHandleSingleQuotedValue()
    {
        // Given
        ListConverter converter = new ListConverter( String.class, NTString, expressionEvaluator() );
        String listString = "['foo', 'bar']";

        // When
        DefaultParameterValue converted = converter.apply( listString );

        // Then
        assertThat( converted, equalTo( ntList( asList( "foo", "bar" ), NTString ) ) );
    }

    @Test
    void shouldHandleDoubleQuotedValue()
    {
        // Given
        ListConverter converter = new ListConverter( String.class, NTString, expressionEvaluator() );
        String listString = "[\"foo\", \"bar\"]";

        // When
        DefaultParameterValue converted = converter.apply( listString );

        // Then
        assertThat( converted, equalTo( ntList( asList( "foo", "bar" ), NTString ) ) );
    }

    @Test
    void shouldHandleIntegerValue()
    {
        // Given
        ListConverter converter = new ListConverter( Long.class, NTInteger, expressionEvaluator() );
        String listString = "[1337, 42]";

        // When
        DefaultParameterValue converted = converter.apply( listString );

        // Then
        assertThat( converted, equalTo( ntList( asList( 1337L, 42L ), NTInteger ) ) );
    }

    @Test
    void shouldHandleFloatValue()
    {
        // Given
        ListConverter converter = new ListConverter( Double.class, NTFloat, expressionEvaluator() );
        String listSting = "[2.718281828, 3.14]";

        // When
        DefaultParameterValue converted = converter.apply( listSting );

        // Then
        assertThat( converted, equalTo( ntList( asList( 2.718281828, 3.14 ), NTFloat ) ) );
    }

    @Test
    void shouldHandleNullValue()
    {
        // Given
        ListConverter converter = new ListConverter( Double.class, NTFloat, expressionEvaluator() );
        String listString = "[null]";

        // When
        DefaultParameterValue converted = converter.apply( listString );

        // Then
        assertThat( converted, equalTo( ntList( singletonList( null ), NTFloat ) ) );
    }

    @Test
    void shouldHandleBooleanValues()
    {
        // Given
        ListConverter converter = new ListConverter( Boolean.class, NTBoolean, expressionEvaluator() );
        String mapString = "[false, true]";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        assertThat( converted, equalTo( ntList( asList( false, true ), NTBoolean ) ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void shouldHandleNestedLists()
    {
        // Given
        ParameterizedType type = mock( ParameterizedType.class );
        when( type.getActualTypeArguments() ).thenReturn( new Type[]{Object.class} );
        ListConverter converter = new ListConverter( type, NTList( NTAny ), expressionEvaluator() );
        String mapString = "[42, [42, 1337]]";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        List<Object> list = (List<Object>) converted.value();
        assertThat( list.get( 0 ), equalTo( 42L ) );
        assertThat( list.get( 1 ), equalTo( asList( 42L, 1337L ) ) );
    }

    @Test
    void shouldFailOnInvalidMixedTypes()
    {
        // Given
        ListConverter converter = new ListConverter( Long.class, NTInteger, expressionEvaluator() );
        String listString = "[1337, 'forty-two']";

        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, () -> converter.apply( listString ) );
        assertThat( exception.getMessage(), equalTo( "Expects a list of Long but got a list of String" ) );
    }

    @Test
    void shouldPassOnValidMixedTypes()
    {
        // Given
        ListConverter converter = new ListConverter( Object.class, NTAny, expressionEvaluator() );
        String listString = "[1337, 'forty-two']";

        // When
        DefaultParameterValue value = converter.apply( listString );

        // Then
        assertThat( value, equalTo( ntList( asList( 1337L, "forty-two" ), NTAny ) ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void shouldHandleListsOfMaps()
    {
        // Given
        ListConverter converter = new ListConverter( Map.class, NTMap, expressionEvaluator() );
        String mapString = "[{k1: 42}, {k1: 1337}]";

        // When
        DefaultParameterValue converted = converter.apply( mapString );

        // Then
        List<Object> list = (List<Object>) converted.value();
        assertThat( list.get( 0 ), equalTo( map( "k1", 42L ) ) );
        assertThat( list.get( 1 ), equalTo( map( "k1", 1337L ) ) );
    }
}
