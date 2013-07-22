/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.neo4j.helpers.collection.IteratorUtil;

import static java.util.Arrays.asList;

public class AllItemsMatcher<T> extends BaseMatcher<Iterable<T>>
{
    private final Iterable<T> expected;
    
    public AllItemsMatcher( T... expected )
    {
        this.expected = asList( expected );
    }

    public AllItemsMatcher( Iterable<T> expected )
    {
        this.expected = expected;
    }
    
    @Override
    public void describeTo( Description description )
    {
        description.appendText( "All items (no more, no less) must be equal to" );
        description.appendValueList( "[", ",", "]", expected );
    }

    @Override
    public boolean matches( Object item )
    {
        Iterable<T> items = (Iterable<T>) item;
        return IteratorUtil.iteratorsEqual( expected.iterator(), items.iterator() );
    }
    
    public static <T> Matcher<Iterable<T>> matchesAll( T... expected )
    {
        return new AllItemsMatcher<>( expected );
    }
    
    public static <T> Matcher<Iterable<T>> matchesAll( Iterable<T> expected )
    {
        return new AllItemsMatcher<>( expected );
    }
}
