/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util.diffsets;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.DiffApplyingPrimitiveIntIterator;
import org.neo4j.kernel.impl.util.DiffApplyingPrimitiveLongIterator;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

/**
 * Given a sequence of add and removal operations, instances of DiffSets track
 * which elements need to actually be added and removed at minimum from some
 * hypothetical target collection such that the result is equivalent to just
 * executing the sequence of additions and removals in order
 *
 * @param <T> type of elements
 */
public class DiffSets<T> extends SuperDiffSets<T,PrimitiveLongIterator> implements ReadableDiffSets<T>
{
    @SuppressWarnings("unchecked")
    public static <T> DiffSets<T> emptyDiffSets()
    {
        return EMPTY;
    }

    public DiffSets()
    {
        this( null, null );
    }

    public DiffSets( Set<T> addedElements, Set<T> removedElements )
    {
        super( addedElements, removedElements );
    }

    @Override
    public PrimitiveLongIterator augment( final PrimitiveLongIterator source )
    {
        return new DiffApplyingPrimitiveLongIterator( source, added( false ), removed( false ) );
    }

    @Override
    public PrimitiveIntIterator augment( final PrimitiveIntIterator source )
    {
        return new DiffApplyingPrimitiveIntIterator( source, added( false ), removed( false ) );
    }

    @Override
    public PrimitiveLongIterator augmentWithRemovals( final PrimitiveLongIterator source )
    {
        return new DiffApplyingPrimitiveLongIterator( source, Collections.emptySet(), removed( false ) );
    }

    @Override
    public PrimitiveLongIterator augmentWithAdditions( final PrimitiveLongIterator source )
    {
        return new DiffApplyingPrimitiveLongIterator( source, added( false ), Collections.emptySet() );
    }

    @Override
    public DiffSets<T> filterAdded( Predicate<T> addedFilter )
    {
        return new DiffSets<>(
                asSet( Iterables.filter( addedFilter, added( false ) ) ),
                asSet( removed( false ) ) );
    }

    @Override
    public DiffSets<T> filter( Predicate<T> filter )
    {
        return new DiffSets<>(
                asSet( Iterables.filter( filter, added( false ) ) ),
                asSet( Iterables.filter( filter, removed( false ) ) ) );
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final DiffSets EMPTY = new DiffSets( Collections.emptySet(), Collections.emptySet() )
    {
        @Override
        public Iterator apply( Iterator source )
        {
            return source;
        }

        @Override
        public PrimitiveLongIterator augment( PrimitiveLongIterator source )
        {
            return source;
        }

        @Override
        public PrimitiveIntIterator augment( PrimitiveIntIterator source )
        {
            return source;
        }

        @Override
        public PrimitiveLongIterator augmentWithRemovals( PrimitiveLongIterator source )
        {
            return source;
        }

        @Override
        public PrimitiveLongIterator augmentWithAdditions( PrimitiveLongIterator source )
        {
            return source;
        }

        @Override
        public DiffSets filterAdded( Predicate addedFilter )
        {
            return this;
        }

        @Override
        public DiffSets filter( Predicate filter )
        {
            return this;
        }
    };
}
