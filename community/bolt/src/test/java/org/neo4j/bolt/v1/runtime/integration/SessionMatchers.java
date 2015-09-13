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
package org.neo4j.bolt.v1.runtime.integration;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.bolt.v1.runtime.spi.Record;

import static java.util.Arrays.asList;

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

    public static Matcher<RecordingCallback.Call> streamContaining( final Matcher<?>... values )
    {
        return new TypeSafeMatcher<RecordingCallback.Call>()
        {
            @Override
            protected boolean matchesSafely( RecordingCallback.Call item )
            {
                if ( !(item instanceof RecordingCallback.Result) )
                {
                    return false;
                }

                Record[] actual = ((RecordingCallback.Result) item).records();
                for ( int i = 0; i < values.length; i++ )
                {
                    if ( !values[i].matches( actual[i] ) )
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
                description.appendList( "values=[", "\n", "]", asList( values ) );
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
}
