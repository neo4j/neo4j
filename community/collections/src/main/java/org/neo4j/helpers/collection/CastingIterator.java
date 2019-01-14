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
package org.neo4j.helpers.collection;

import java.util.Iterator;

/**
 * An iterator which filters for elements of a given subtype, and casts to this type.
 *
 * @param <T> the type of elements returned by this iterator.
 * @param <A> the type of elements read by this iterator. This must be a supertype of T.
 */
public class CastingIterator<T extends A, A> extends PrefetchingIterator<T>
{
    private final Iterator<A> source;
    private Class<T> outClass;

    public CastingIterator( Iterator<A> source, Class<T> outClass )
    {
        this.source = source;
        this.outClass = outClass;
    }

    @Override
    protected T fetchNextOrNull()
    {
        while ( source.hasNext() )
        {
            A testItem = source.next();
            if ( outClass.isInstance( testItem ) )
            {
                return outClass.cast( testItem );
            }
        }
        return null;
    }
}
