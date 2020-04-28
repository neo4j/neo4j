/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.helpers;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.common.Validator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ArgsTest
{
    @Test
    void shouldInterpretSingleDashAsValue()
    {
        // GIVEN
        Args args = Args.parse( "-test", "-" );

        // WHEN
        String value = args.get( "test" );

        // THEN
        assertEquals( "-", value );
    }

    @Test
    void testInterleavedParametersWithValuesAndNot()
    {
        String[] line = {"-host", "machine.foo.com", "-port", "1234", "-v", "-name", "othershell"};
        Args args = Args.parse( line );
        assertEquals( "machine.foo.com", args.get( "host", null ) );
        assertEquals( "1234", args.get( "port", null ) );
        assertEquals( 1234, args.getNumber( "port", null ).intValue() );
        assertEquals( "othershell", args.get( "name", null ) );
        assertTrue( args.has( "v" ) );
        assertTrue( args.orphans().isEmpty() );
    }

    @Test
    void testInterleavedEqualsArgsAndSplitKeyValue()
    {
        String[] line = {"-host=localhost", "-v", "--port", "1234", "param1", "-name=Something", "param2"};
        Args args = Args.parse( line );
        assertEquals( "localhost", args.get( "host", null ) );
        assertTrue( args.has( "v" ) );
        assertEquals( 1234, args.getNumber( "port", null ).intValue() );
        assertEquals( "Something", args.get( "name", null ) );

        assertEquals( 2, args.orphans().size() );
        assertEquals( "param1", args.orphans().get( 0 ) );
        assertEquals( "param2", args.orphans().get( 1 ) );
    }

    @Test
    void testParameterWithDashValue()
    {
        String[] line = {"-file", "-"};
        Args args = Args.parse( line );
        assertEquals( 1, args.asMap().size() );
        assertEquals( "-", args.get( "file", null ) );
        assertTrue( args.orphans().isEmpty() );
    }

    @Test
    void testEnum()
    {
        String[] line = {"--enum=" + MyEnum.second.name()};
        Args args = Args.parse( line );
        Enum<MyEnum> result = args.getEnum( MyEnum.class, "enum", MyEnum.first );
        assertEquals( MyEnum.second, result );
    }

    @Test
    void testEnumWithDefault()
    {
        String[] line = {};
        Args args = Args.parse( line );
        MyEnum result = args.getEnum( MyEnum.class, "enum", MyEnum.third );
        assertEquals( MyEnum.third, result );
    }

    @Test
    void testEnumWithInvalidValue()
    {
        String[] line = {"--myenum=something"};
        Args args = Args.parse( line );
        assertThrows( IllegalArgumentException.class, () -> args.getEnum( MyEnum.class, "myenum", MyEnum.third ) );
    }

    @Test
    void shouldInterpretOption()
    {
        // GIVEN
        int expectedValue = 42;
        Args args = Args.parse( "--arg", String.valueOf( expectedValue ) );
        @SuppressWarnings( "unchecked" )
        Validator<Integer> validator = mock( Validator.class );

        // WHEN
        int value = args.interpretOption( "arg", key ->
        {
            throw new IllegalArgumentException( "Missing argument '" + key + '\'' );
        }, Integer::parseInt, validator );

        // THEN
        assertEquals( expectedValue, value );
        verify( validator ).validate( expectedValue );
    }

    @Test
    void shouldInterpretOrphan()
    {
        // GIVEN
        int expectedValue = 42;
        Args args = Args.parse( String.valueOf( expectedValue ) );
        @SuppressWarnings( "unchecked" )
        Validator<Integer> validator = mock( Validator.class );

        // WHEN
        int value = args.interpretOrphan( 0, key ->
        {
            throw new IllegalArgumentException( "Missing argument '" + key + '\'' );
        }, Integer::parseInt, validator );

        // THEN
        assertEquals( expectedValue, value );
        verify( validator ).validate( expectedValue );
    }

    @Test
    void shouldInterpretMultipleOptionValues()
    {
        // GIVEN
        Collection<Integer> expectedValues = Arrays.asList( 12, 34, 56 );
        List<String> argList = new ArrayList<>();
        String key = "number";
        for ( int value : expectedValues )
        {
            argList.add( "--" + key );
            argList.add( String.valueOf( value ) );
        }
        Args args = Args.parse( argList.toArray( new String[0] ) );

        // WHEN
        assertThrows( IllegalArgumentException.class, () -> args.get( key ) );
        Collection<Integer> numbers = args.interpretOptions( key, k -> null, Integer::parseInt );

        // THEN
        assertEquals( expectedValues, numbers );
    }

    @Test
    void testBooleanWithDefault()
    {
        // Given
        Args args = Args.parse( "--no_value" );

        // When & then
        assertThat( args.getBoolean( "not_set", true, true ) ).isTrue();
        assertThat( args.getBoolean( "not_set", false, true ) ).isFalse();
        assertThat( args.getBoolean( "not_set", false, false ) ).isFalse();
        assertThat( args.getBoolean( "not_set", true, false ) ).isTrue();

        assertThat( args.getBoolean( "no_value", true, true ) ).isTrue();
        assertThat( args.getBoolean( "no_value", false, true ) ).isTrue();
        assertThat( args.getBoolean( "no_value", false, false ) ).isFalse();
        assertThat( args.getBoolean( "no_value", true, false ) ).isFalse();
    }

    @Test
    void shouldGetAsMap()
    {
        // GIVEN
        Args args = Args.parse( "--with-value", "value", "--without-value" );

        // WHEN
        Map<String, String> map = args.asMap();

        // THEN

        assertThat( map ).hasSize( 2 ).containsEntry( "with-value", "value" ).containsEntry( "without-value", null );
    }

    @Test
    void shouldInterpretOptionMetadata()
    {
        // GIVEN
        Args args = Args.parse( "--my-option:Meta", "my value", "--my-option:Other", "other value" );

        // WHEN
        Collection<Args.Option<String>> options = args.interpretOptionsWithMetadata( "my-option",
                key ->
                {
                    throw new IllegalArgumentException( "Missing argument '" + key + '\'' );
                }, value -> value );

        // THEN
        assertEquals( 2, options.size() );
        Iterator<Args.Option<String>> optionIterator = options.iterator();
        Args.Option<String> first = optionIterator.next();
        assertEquals( "my value", first.value() );
        assertEquals( "Meta", first.metadata() );
        Args.Option<String> second = optionIterator.next();
        assertEquals( "other value", second.value() );
        assertEquals( "Other", second.metadata() );
    }

    @Test
    void shouldHandleLastOrphanParam()
    {
        // Given
        Args args = Args.withFlags( "recovery" ).parse( "--recovery", "/tmp/neo4j" );

        // When
        List<String> orphans = args.orphans();

        // Then
        assertEquals( Arrays.asList( "/tmp/neo4j" ), orphans );
    }

    @Test
    void shouldHandleOnlyFlagsAndNoArgs()
    {
        // Given
        Args args = Args.withFlags( "foo", "bar" ).parse( "-foo", "--bar" );

        // When
        List<String> orphans = args.orphans();

        // Then
        assertEquals( Collections.<String>emptyList(), orphans );
        assertTrue( args.getBoolean( "foo", false, true ) );
        assertTrue( args.getBoolean( "bar", false, true ) );
    }

    @Test
    void shouldStillAllowExplicitValuesForFlags()
    {
        // Given
        Args args = Args.withFlags( "foo", "bar" ).parse( "-foo=false", "--bar" );

        // When
        List<String> orphans = args.orphans();

        // Then
        assertEquals( Arrays.<String>asList(), orphans );
        assertFalse( args.getBoolean( "foo", false, false ) );
        assertTrue( args.getBoolean( "bar", false, true ) );
    }

    @Test
    void shouldHandleMixtureOfFlagsAndOrphanParams()
    {
        // Given
        Args args = Args.withFlags( "big", "soft", "saysMeow" ).parse(
                "-big", "-size=120", "-soft=true", "withStripes", "-saysMeow=false", "-name=ShereKhan", "badTiger" );

        // When
        List<String> orphans = args.orphans();

        // Then
        assertEquals( Arrays.asList( "withStripes", "badTiger" ), orphans );

        assertEquals( 120, args.getNumber( "size", 0 ).intValue() );
        assertEquals( "ShereKhan", args.get( "name" ) );

        assertTrue( args.getBoolean( "big", false, true ) );
        assertTrue( args.getBoolean( "soft", false, false ) );
        assertFalse( args.getBoolean( "saysMeow", true, true ) );
    }

    @Test
    void shouldHandleFlagSpecifiedAsLastArgument()
    {
        // Given
        Args args = Args.withFlags( "flag1", "flag2" ).parse(
                "-key=Foo", "-flag1", "false", "-value", "Bar", "-flag2", "false" );

        // When
        List<String> orphans = args.orphans();

        // Then
        assertTrue( orphans.isEmpty(), "Orphan args expected to be empty, but were: " + orphans );
        assertEquals( "Foo", args.get( "key" ) );
        assertEquals( "Bar", args.get( "value" ) );
        assertFalse( args.getBoolean( "flag1", true ) );
        assertFalse( args.getBoolean( "flag2", true ) );
    }

    @Test
    void shouldRecognizeFlagsOfAnyForm()
    {
        // Given
        Args args = Args.withFlags( "flag1", "flag2", "flag3" ).parse(
                "-key1=Foo", "-flag1", "-key1", "Bar", "-flag2=true", "-key3=Baz", "-flag3", "true" );

        // When
        List<String> orphans = args.orphans();

        // Then
        assertTrue( orphans.isEmpty(), "Orphan args expected to be empty, but were: " + orphans );
        assertTrue( args.getBoolean( "flag1", false, true ) );
        assertTrue( args.getBoolean( "flag2", false, false ) );
        assertTrue( args.getBoolean( "flag3", false, false ) );
    }

    @Test
    void shouldReturnEmptyCollectionForOptionalMissingOption()
    {
        // Given
        Args args = Args.withFlags().parse();

        // When
        Collection<String> interpreted = args.interpretOptions( "something", x -> null,
                value -> value );

        // Then
        assertTrue( interpreted.isEmpty() );
    }

    private enum MyEnum
    {
        first,
        second,
        third
    }
}
