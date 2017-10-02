/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public class ExceptionMatchers
{
    public static TypeSafeMatcher<Throwable> exceptionContainsSuppressedThrowable( Throwable expectedSuppressed )
    {
        return new TypeSafeMatcher<Throwable>()
        {
            @Override
            protected boolean matchesSafely( Throwable item )
            {
                List<Throwable> suppress = Arrays.asList( item.getSuppressed() );
                return suppress.contains( expectedSuppressed );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "contains suppressed exception " ).appendValue( expectedSuppressed );
            }

            @Override
            protected void describeMismatchSafely( Throwable item, Description mismatchDescription )
            {
                List<String> suppressedExceptionStrings = Stream
                        .of( item.getSuppressed() )
                        .map( ExceptionMatchers::exceptionWithMessageToString )
                        .collect( Collectors.toList() );
                mismatchDescription
                        .appendText( "exception " )
                        .appendValue( item )
                        .appendText( " with suppressed " )
                        .appendValueList( "[", ", ", "]", suppressedExceptionStrings )
                        .appendText( " does not contain " )
                        .appendValue( exceptionWithMessageToString( expectedSuppressed ) );
            }
        };
    }

    private static String exceptionWithMessageToString( Throwable throwable )
    {
        return format( "<%s:%s>", throwable.getClass(), throwable.getMessage() );
    }
}
