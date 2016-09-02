/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.test.matchers;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class CommonMatchers
{
    private CommonMatchers()
    {
    }

    /**
     * Checks that an iterable of T contains items that each match one and only one of the provided matchers and
     * and that each matchers matches exactly one and only one of the items from the iterable.
     */
    @SafeVarargs
    public static <T> Matcher<? super Iterable<T>> matchesOneToOneInAnyOrder( Matcher<? super T>... expectedMatchers )
    {
        return new MatchesOneToOneInAnyOrder<>( expectedMatchers );
    }

    private static class MatchesOneToOneInAnyOrder<T> extends TypeSafeMatcher<Iterable<T>>
    {
        private final Matcher<? super T>[] expectedMatchers;

        @SafeVarargs
        MatchesOneToOneInAnyOrder( Matcher<? super T>... expectedMatchers )
        {
            this.expectedMatchers = expectedMatchers;
        }

        @Override
        protected boolean matchesSafely( Iterable<T> items )
        {
            Set<Matcher<? super T>> matchers = uniqueMatchers();

            for ( T item : items )
            {
                Matcher<? super T> matcherFound = null;
                for ( Matcher<? super T> matcherConsidered : matchers )
                {
                    if ( matcherConsidered.matches( item ) )
                    {
                        if ( matcherFound == null )
                        {
                            matcherFound = matcherConsidered;
                        }
                        else
                        {
                            return false;
                        }
                    }
                }
                if ( matcherFound == null )
                {
                    return false;
                }
                else
                {
                    matchers.remove( matcherFound );
                }
            }

            return matchers.isEmpty();
        }

        @Override
        public void describeTo( Description description )
        {
            Set<Matcher<? super T>> matchers = uniqueMatchers();

            description.appendText( "items that each match exactly one of " );
            description.appendList( "{ ", ", ", " }", matchers );
            description.appendText( " and exactly as many items as matchers" );
        }

        private Set<Matcher<? super T>> uniqueMatchers()
        {
            Set<Matcher<? super T>> matchers = new HashSet<>();
            Collections.addAll( matchers, expectedMatchers );
            return matchers;
        }
    }
}
