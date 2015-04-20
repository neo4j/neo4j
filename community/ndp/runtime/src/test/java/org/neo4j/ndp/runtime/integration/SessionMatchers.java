/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.runtime.integration;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.ndp.runtime.StatementMetadata;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class SessionMatchers
{
    public static Matcher<RecordingCallback.Call> success()
    {
        return new TypeSafeMatcher<RecordingCallback.Call>()
        {
            @Override
            protected boolean matchesSafely( RecordingCallback.Call item )
            {
                return item.isSuccess();
            }

            @Override
            public void describeTo( Description description )
            {

            }
        };
    }

    public static Matcher<RecordingCallback.Call> statementMetadata( final String[] fieldNames )
    {
        return new TypeSafeMatcher<RecordingCallback.Call>()
        {
            @Override
            protected boolean matchesSafely( RecordingCallback.Call item )
            {
                if(!(item instanceof RecordingCallback.StatementSuccess))
                {
                    return false;
                }

                StatementMetadata meta = ((RecordingCallback.StatementSuccess) item).meta();

                assertTrue( Arrays.toString(fieldNames) + " == " + Arrays.toString( meta.fieldNames() ),
                        Arrays.equals( fieldNames, meta.fieldNames() ));

                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValueList( "StatementMetadata[", ",", "]", fieldNames );
            }
        };
    }

    public static Matcher<RecordingCallback> callsWere( final Matcher<RecordingCallback.Call> ... calls )
    {
        return new TypeSafeMatcher<RecordingCallback>()
        {
            @Override
            protected boolean matchesSafely( RecordingCallback item )
            {
                try
                {
                    for ( int i = 0; i < calls.length; i++ )
                    {
                        assertThat( item.next(), calls[i] );
                    }
                }
                catch(InterruptedException e)
                {
                    throw new RuntimeException( e );
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendList( "Calls[",",","]", asList(calls) );
            }
        };
    }

    public static Matcher<RecordingCallback.Call> streamContaining( final Matcher<?>... values )
    {
        return new TypeSafeMatcher<RecordingCallback.Call>()
        {
            @Override
            protected boolean matchesSafely( RecordingCallback.Call item )
            {
                if(!(item instanceof RecordingCallback.Result) )
                {
                    return false;
                }

                Object[] actual = ((RecordingCallback.Result) item).records();
                for ( int i = 0; i < values.length; i++ )
                {
                    if(!values[i].matches( actual[i] ))
                    {
                        return false;
                    }
                }

                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Stream{" );
                description.appendList( "values=[", "\n", "]" , asList(values));
                description.appendText( "}" );
            }
        };
    }

    public static Matcher<? super RecordingCallback.Call> failedWith( final Status expected )
    {
        return new TypeSafeMatcher<RecordingCallback.Call>()
        {
            @Override
            protected boolean matchesSafely( RecordingCallback.Call item )
            {
                return expected == item.error().status();

            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( expected.toString() );
            }
        };
    }

    public static Matcher<? super RecordingCallback.Call> ignored()
    {
        return new TypeSafeMatcher<RecordingCallback.Call>()
        {
            @Override
            protected boolean matchesSafely( RecordingCallback.Call item )
            {
                return item.isIgnored();

            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "ignored" );
            }
        };
    }

    public static Matcher<Map<String, Object>> mapMatcher( final Object... alternatingKeyValue )
    {
        final Map<String, Object> expected = MapUtil.map( alternatingKeyValue );
        return new TypeSafeMatcher<Map<String, Object>>()
        {
            @Override
            protected boolean matchesSafely( Map<String, Object> item )
            {
                assertThat( item.entrySet(), equalTo(expected.entrySet()) );
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( expected.toString() );
            }
        };
    }
}
