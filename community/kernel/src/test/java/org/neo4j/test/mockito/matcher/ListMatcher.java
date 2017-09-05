/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.test.mockito.matcher;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * An org.hamcrest Matcher that matches {@link Collection collections}.
 * @param <T> The parameter of the Collection to match
 */
public class ListMatcher<T> extends TypeSafeMatcher<List<T>>
{
    private final List<T> toMatch;

    private ListMatcher( List<T> toMatch )
    {
        this.toMatch = toMatch;
    }

    @Override
    protected boolean matchesSafely( List<T> objects )
    {
        return IterableMatcher.itemsMatches( toMatch, objects );
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendValueList( "Collection [",  ",", "]", toMatch );
    }

    public static <T> ListMatcher<T> matchesList( List<T> toMatch )
    {
        return new ListMatcher<>( toMatch );
    }

    @SafeVarargs
    public static <T> ListMatcher<T> matchesList( T... toMatch )
    {
        return new ListMatcher<>( asList( toMatch ) );
    }
}
