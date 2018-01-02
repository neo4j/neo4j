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
package org.neo4j.test;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.Strings;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class Assert
{
    private Assert()
    {
    }

    public static void assertObjectOrArrayEquals( Object expected, Object actual )
    {
        assertObjectOrArrayEquals( "", expected, actual );
    }

    public static void assertObjectOrArrayEquals( String message, Object expected, Object actual )
    {
        if ( expected.getClass().isArray() )
        {
            if ( !ArrayUtil.equals( expected, actual ) )
            {
                throw newAssertionError( message, expected, actual );
            }
        }
        else
        {
            if ( !Objects.equals( expected, actual ) )
            {
                throw newAssertionError( message, expected, actual );
            }
        }
    }

    public static <T, E extends Exception> void assertEventually(
            String reason, ThrowingSupplier<T, E> actual, Matcher<? super T> matcher, long timeout, TimeUnit timeUnit
    ) throws E
    {
        long waitUntil = System.currentTimeMillis() + timeUnit.toMillis( timeout );
        boolean matched;
        while ( !( matched = matcher.matches( actual.get() ) ) && System.currentTimeMillis() <= waitUntil )
        {
            try
            {
                Thread.sleep( 250 );
            } catch ( InterruptedException e )
            {
                // ignored
            }
        }

        if ( !matched )
        {
            Description description = new StringDescription();
            description.appendText( reason )
                    .appendText( "\nExpected: " )
                    .appendDescriptionOf( matcher )
                    .appendText( "\n     but: " );
            matcher.describeMismatch( actual, description );

            throw new AssertionError( "Timeout hit (" + timeout + " " + timeUnit.toString().toLowerCase() +
                    ") while waiting for condition to match: " + description.toString() );
        }
    }

    private static AssertionError newAssertionError( String message, Object expected, Object actual )
    {
        return new AssertionError( ((message == null || message.isEmpty()) ? "" : message + "\n") +
                                   "Expected: " + Strings.prettyPrint( expected ) +
                                   ", actual: " + Strings.prettyPrint( actual ) );
    }
}
