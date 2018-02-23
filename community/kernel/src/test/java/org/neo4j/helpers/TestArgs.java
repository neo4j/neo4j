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
package org.neo4j.helpers;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.Args.Option;
import org.neo4j.kernel.impl.util.Validator;

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.Args.parse;
import static org.neo4j.helpers.Args.withFlags;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.Converters.mandatory;
import static org.neo4j.kernel.impl.util.Converters.optional;
import static org.neo4j.kernel.impl.util.Converters.toInt;

public class TestArgs
{
    @Test
    public void testInterleavedParametersWithValuesAndNot()
    {
        String[] line = { "-host", "machine.foo.com", "-port", "1234", "-v", "-name", "othershell" };
        Args args = parse( line );
        assertEquals( "machine.foo.com", args.get( "host", null ) );
        assertEquals( "1234", args.get( "port", null ) );
        assertEquals( 1234, args.getNumber( "port", null ).intValue() );
        assertEquals( "othershell", args.get( "name", null ) );
        assertTrue( args.has( "v" ) );
        assertTrue( args.orphans().isEmpty() );
    }

    @Test
    public void testInterleavedEqualsArgsAndSplitKeyValue()
    {
        String[] line = { "-host=localhost", "-v", "--port", "1234", "param1", "-name=Something", "param2" };
        Args args = parse( line );
        assertEquals( "localhost", args.get( "host", null ) );
        assertTrue( args.has( "v" ) );
        assertEquals( 1234, args.getNumber( "port", null ).intValue() );
        assertEquals( "Something", args.get( "name", null ) );

        assertEquals( 2, args.orphans().size() );
        assertEquals( "param1", args.orphans().get( 0 ) );
        assertEquals( "param2", args.orphans().get( 1 ) );
    }

    @Test
    public void testParameterWithDashValue()
    {
        String [] line = { "-file", "-" };
        Args args = parse( line );
        assertEquals( 1, args.asMap().size() );
        assertEquals( "-", args.get( "file", null ) );
        assertTrue( args.orphans().isEmpty() );
    }

    @Test
    public void testEnum()
    {
        String[] line = { "--enum=" + MyEnum.second.name() };
        Args args = parse( line );
        Enum<MyEnum> result = args.getEnum( MyEnum.class, "enum", MyEnum.first );
        assertEquals( MyEnum.second, result );
    }

    @Test
    public void testEnumWithDefault()
    {
        String[] line = {};
        Args args = parse( line );
        MyEnum result = args.getEnum( MyEnum.class, "enum", MyEnum.third );
        assertEquals( MyEnum.third, result );
    }

    @Test
    public void testEnumWithInvalidValue()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            String[] line = {"--myenum=something"};
            Args args = parse( line );
            args.getEnum( MyEnum.class, "myenum", MyEnum.third );
        } );
    }

    @Test
    public void shouldInterpretOption()
    {
        // GIVEN
        int expectedValue = 42;
        Args args = parse( "--arg", valueOf( expectedValue ));
        @SuppressWarnings( "unchecked" )
        Validator<Integer> validator = mock( Validator.class );

        // WHEN
        int value = args.interpretOption( "arg", mandatory(), toInt(), validator );

        // THEN
        assertEquals( expectedValue, value );
        verify( validator ).validate( expectedValue );
    }

    @Test
    public void shouldInterpretOrphan()
    {
        // GIVEN
        int expectedValue = 42;
        Args args = parse( valueOf( expectedValue ) );
        @SuppressWarnings( "unchecked" )
        Validator<Integer> validator = mock( Validator.class );

        // WHEN
        int value = args.interpretOrphan( 0, mandatory(), toInt(), validator );

        // THEN
        assertEquals( expectedValue, value );
        verify( validator ).validate( expectedValue );
    }

    @Test
    public void shouldInterpretMultipleOptionValues()
    {
        // GIVEN
        Collection<Integer> expectedValues = asList( 12, 34, 56 );
        List<String> argList = new ArrayList<>();
        String key = "number";
        for ( int value : expectedValues )
        {
            argList.add( "--" + key );
            argList.add( valueOf( value ) );
        }
        Args args = parse( argList.toArray( new String[argList.size()] ) );

        // WHEN
        try
        {
            args.get( key );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {   // Good
        }

        Collection<Integer> numbers = args.interpretOptions( key, optional(),
                toInt() );

        // THEN
        assertEquals( expectedValues, numbers );
    }

    @Test
    public void testBooleanWithDefault()
    {
        // Given
        Args args = parse( "--no_value" );

        // When & then
        assertThat(args.getBoolean( "not_set", true, true ), equalTo(true));
        assertThat(args.getBoolean( "not_set", false, true ), equalTo(false));
        assertThat(args.getBoolean( "not_set", false, false ), equalTo(false));
        assertThat(args.getBoolean( "not_set", true, false ), equalTo(true));

        assertThat(args.getBoolean( "no_value", true, true ), equalTo(true));
        assertThat(args.getBoolean( "no_value", false, true ), equalTo(true));
        assertThat(args.getBoolean( "no_value", false, false ), equalTo(false));
        assertThat(args.getBoolean( "no_value", true, false ), equalTo(false));
    }

    @Test
    public void shouldGetAsMap()
    {
        // GIVEN
        Args args = parse( "--with-value", "value", "--without-value" );

        // WHEN
        Map<String,String> map = args.asMap();

        // THEN
        assertEquals( stringMap( "with-value", "value", "without-value", null ), map );
    }

    @Test
    public void shouldInterpretOptionMetadata()
    {
        // GIVEN
        Args args = parse( "--my-option:Meta", "my value", "--my-option:Other", "other value" );

        // WHEN
        Collection<Option<String>> options = args.interpretOptionsWithMetadata( "my-option",
                mandatory(), value -> value );

        // THEN
        assertEquals( 2, options.size() );
        Iterator<Option<String>> optionIterator = options.iterator();
        Option<String> first = optionIterator.next();
        assertEquals( "my value", first.value() );
        assertEquals( "Meta", first.metadata() );
        Option<String> second = optionIterator.next();
        assertEquals( "other value", second.value() );
        assertEquals( "Other", second.metadata() );
    }

    @Test
    public void shouldHandleLastOrphanParam()
    {
        // Given
        Args args = withFlags("recovery").parse( "--recovery", "/tmp/graph.db" );

        // When
        List<String> orphans = args.orphans();

        // Then
        assertEquals( asList( "/tmp/graph.db" ), orphans );
    }

    @Test
    public void shouldHandleOnlyFlagsAndNoArgs()
    {
        // Given
        Args args = withFlags( "foo", "bar" ).parse("-foo", "--bar");

        // When
        List<String> orphans = args.orphans();

        // Then
        assertEquals( Collections.<String>emptyList(), orphans );
        assertTrue( args.getBoolean( "foo", false, true ) );
        assertTrue( args.getBoolean( "bar", false, true ) );
    }

    @Test
    public void shouldStillAllowExplicitValuesForFlags()
    {
        // Given
        Args args = withFlags( "foo", "bar" ).parse("-foo=false", "--bar");

        // When
        List<String> orphans = args.orphans();

        // Then
        assertEquals( Arrays.<String>asList() , orphans );
        assertFalse( args.getBoolean( "foo", false, false ) );
        assertTrue( args.getBoolean( "bar", false, true ) );
    }

    @Test
    public void shouldHandleMixtureOfFlagsAndOrphanParams()
    {
        // Given
        Args args = withFlags( "big", "soft", "saysMeow" ).parse(
                "-big", "-size=120", "-soft=true", "withStripes", "-saysMeow=false", "-name=ShereKhan", "badTiger" );

        // When
        List<String> orphans = args.orphans();

        // Then
        assertEquals( asList( "withStripes", "badTiger" ), orphans );

        assertEquals( 120, args.getNumber( "size", 0 ).intValue() );
        assertEquals( "ShereKhan", args.get( "name" ) );

        assertTrue( args.getBoolean( "big", false, true ) );
        assertTrue( args.getBoolean( "soft", false, false ) );
        assertFalse( args.getBoolean( "saysMeow", true, true ) );
    }

    @Test
    public void shouldHandleFlagSpecifiedAsLastArgument()
    {
        // Given
        Args args = withFlags( "flag1", "flag2" ).parse(
                "-key=Foo", "-flag1", "false", "-value", "Bar", "-flag2", "false" );

        // When
        List<String> orphans = args.orphans();

        // Then
        assertTrue( orphans.isEmpty(), "Orphan args expected to be empty, but were: " + orphans );
        assertEquals( "Foo", args.get( "key" ) );
        assertEquals( "Bar", args.get( "value" ) );
        assertFalse( args.getBoolean( "flag1", true ), "flag1" );
        assertFalse( args.getBoolean( "flag2", true ), "flag1" );
    }

    @Test
    public void shouldRecognizeFlagsOfAnyForm()
    {
        // Given
        Args args = withFlags( "flag1", "flag2", "flag3" ).parse(
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
    public void shouldReturnEmptyCollectionForOptionalMissingOption()
    {
        // Given
        Args args = withFlags().parse();

        // When
        Collection<String> interpreted = args.interpretOptions( "something", optional(),
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
