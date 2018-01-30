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
package org.neo4j.kernel.impl.util.diffsets;

import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;

/**
 * Given a sequence of add and removal operations, instances of DiffSets track
 * which elements need to actually be added and removed at minimum from some
 * hypothetical target collection such that the result is equivalent to just
 * executing the sequence of additions and removals in order
 *
 * @param <T> type of elements
 */
public class DiffSets<T> extends SuperDiffSets<T,PrimitiveLongResourceIterator, PrimitiveLongIterator> implements ReadableDiffSets<T>
{
    public DiffSets()
    {
        this( null, null );
    }

    public DiffSets( Set<T> addedElements, Set<T> removedElements )
    {
        super( addedElements, removedElements );
    }

    @Override
    public PrimitiveLongResourceIterator augment( final PrimitiveLongIterator source )
    {
        return new DiffApplyingLongIterator( source, added( false ), removed( false ) );
    }

    @Override
    public PrimitiveIntIterator augment( final PrimitiveIntIterator source )
    {
        return new DiffApplyingIntIterator( source, added( false ), removed( false ) );
    }

    @Override
    public PrimitiveLongResourceIterator augmentWithRemovals( final PrimitiveLongIterator source )
    {
        return new DiffApplyingLongIterator( source, Collections.emptySet(), removed( false ) );
    }

    @Override
    public DiffSets<T> filterAdded( Predicate<T> addedFilter )
    {
        return new DiffSets<>(
                Iterables.asSet( Iterables.filter( addedFilter, added( false ) ) ),
                Iterables.asSet( removed( false ) ) );
    }

    public DiffSets filterType( Class<? extends T> type )
    {
        return new DiffSets<>(
                Iterables.asSet( Iterables.filter( type::isInstance, added( false ) ) ),
                Iterables.asSet( Iterables.filter( type::isInstance,removed( false ) ) ) );
    }
}
