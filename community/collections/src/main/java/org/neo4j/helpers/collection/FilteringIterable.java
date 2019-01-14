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
import java.util.function.Predicate;

import org.neo4j.function.Predicates;

/**
 * An iterable which filters another iterable, only letting items with certain
 * criteria pass through. All iteration/filtering is done lazily.
 *
 * @param <T> the type of items in the iteration.
 */
public class FilteringIterable<T> implements Iterable<T>
{
    private final Iterable<T> source;
    private final Predicate<T> predicate;

    public FilteringIterable( Iterable<T> source, Predicate<T> predicate )
    {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public Iterator<T> iterator()
    {
        return new FilteringIterator<>( source.iterator(), predicate );
    }

    public static <T> Iterable<T> notNull( Iterable<T> source )
    {
        return new FilteringIterable<>( source, Predicates.notNull() );
    }

    public static <T> Iterable<T> noDuplicates( Iterable<T> source )
    {
        return new FilteringIterable<>( source, Predicates.noDuplicates() );
    }
}
