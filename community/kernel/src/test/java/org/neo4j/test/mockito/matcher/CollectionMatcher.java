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
package org.neo4j.test.mockito.matcher;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Collection;
import static java.util.Arrays.asList;

/**
 * An org.hamcrest Matcher that matches {@link Collection collections}.
 * @param <T> The parameter of the Collection to match
 */
public class CollectionMatcher<T> extends TypeSafeMatcher<Collection<T>>
{
    private final Collection<T> toMatch;

    private CollectionMatcher( Collection<T> toMatch )
    {
        this.toMatch = toMatch;
    }

    @Override
    protected boolean matchesSafely( Collection<T> objects )
    {
        return IterableMatcher.itemsMatches( toMatch, objects );
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendValueList( "Collection [",  ",", "]", toMatch );
    }

    public static <T> CollectionMatcher<T> matchesCollection( Collection<T> toMatch )
    {
        return new CollectionMatcher<>( toMatch );
    }

    @SafeVarargs
    public static <T> CollectionMatcher<T> matchesCollection( T... toMatch )
    {
        return new CollectionMatcher<>( asList( toMatch ) );
    }
}
