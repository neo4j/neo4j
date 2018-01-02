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
package org.neo4j.index.impl.lucene;

import java.util.Collection;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.IteratorUtil;

public class Contains<T> extends TypeSafeMatcher<IndexHits<T>>
{
    private final T[] expectedItems;
    private String message;

    public Contains( T... expectedItems )
    {
        this.expectedItems = expectedItems;
    }

    @Override
    public boolean matchesSafely( IndexHits<T> indexHits )
    {
        Collection<T> collection = IteratorUtil.asCollection( indexHits.iterator() );

        if ( expectedItems.length != collection.size() )
        {
            message = "IndexHits with a size of " + expectedItems.length + ", got one with " + collection.size();
            message += collection.toString();
            return false;
        }

        for ( T item : expectedItems )
        {
            if ( !collection.contains( item ) )
            {
                message = "Item (" + item + ") not found.";
                return false;
            }

        }
        return true;
    }

    @Override
    public void describeTo( Description description )
    {
        if (message != null)
        {
            description.appendText( message );
        }
    }

    @Factory
    public static <T> Contains<T> contains( T... expectedItems )
    {
        return new Contains<T>( expectedItems );
    }
}
