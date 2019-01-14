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
package org.neo4j.values.storable;

import java.util.LinkedHashMap;
import java.util.Map;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.values.StructureBuilder;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public final class AssertingStructureBuilder<Input, Result> implements StructureBuilder<Input,Result>
{
    public static <I, O> AssertingStructureBuilder<I,O> asserting( StructureBuilder<I,O> builder )
    {
        return new AssertingStructureBuilder<>( builder );
    }

    public static Matcher<Exception> exception( Class<? extends Exception> type, String message )
    {
        return exception( type, equalTo( message ) );
    }

    public static Matcher<Exception> exception( Class<? extends Exception> type, Matcher<String> message )
    {
        return new TypeSafeMatcher<Exception>( type )
        {
            @Override
            protected boolean matchesSafely( Exception item )
            {
                return message.matches( item.getMessage() );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Exception of type " ).appendValue( type.getName() )
                        .appendText( " with message " ).appendDescriptionOf( message );
            }
        };
    }

    private final Map<String,Input> input = new LinkedHashMap<>();
    private final StructureBuilder<Input,Result> builder;

    private AssertingStructureBuilder( StructureBuilder<Input,Result> builder )
    {
        this.builder = builder;
    }

    public void assertThrows( Class<? extends Exception> type, String message )
    {
        assertThrows( exception( type, message ) );
    }

    public void assertThrows( Class<? extends Exception> type, Matcher<String> message )
    {
        assertThrows( exception( type, message ) );
    }

    public void assertThrows( Matcher<Exception> matches )
    {
        try
        {
            for ( Map.Entry<String,Input> entry : input.entrySet() )
            {
                builder.add( entry.getKey(), entry.getValue() );
            }
            builder.build();
        }
        catch ( Exception expected )
        {
            assertThat( expected, matches );
            return;
        }
        fail( "expected exception" );
    }

    @Override
    public AssertingStructureBuilder<Input,Result> add( String field, Input value )
    {
        input.put( field, value );
        return this;
    }

    @Override
    public Result build()
    {
        throw new UnsupportedOperationException( "do not use this method" );
    }
}
