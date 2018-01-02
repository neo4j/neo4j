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

import java.util.Iterator;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.neo4j.helpers.collection.Iterables;

/**
 * An org.hamcrest Matcher that matches Iterables.
 * @param <T> The parameter of the Iterable to match
 */
public class IterableMatcher<T> extends TypeSafeMatcher<Iterable<T>>
{
    private final Iterable<T> toMatch;

    private IterableMatcher( Iterable<T> toMatch )
    {
        this.toMatch = toMatch;
    }

    @Override
    protected boolean matchesSafely( Iterable<T> objects )
    {
        if ( Iterables.count( toMatch ) != Iterables.count( objects ))
        {
            return false;
        }
        Iterator<T> original = toMatch.iterator();
        Iterator<T> matched = objects.iterator();
        T fromOriginal;
        T fromToMatch;
        for ( ; original.hasNext() && matched.hasNext(); )
        {
            fromOriginal = original.next();
            fromToMatch = matched.next();
            if ( !fromOriginal.equals( fromToMatch ) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendValueList( "Iterable [",  ",", "]", toMatch );
    }

    public static <T> IterableMatcher<T> matchesIterable( Iterable<T> toMatch )
    {
        return new IterableMatcher<T>( toMatch );
    }
}
