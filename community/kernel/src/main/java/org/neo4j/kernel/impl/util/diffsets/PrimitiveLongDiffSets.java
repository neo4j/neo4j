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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.storageengine.api.txstate.PrimitiveLongDiffSetsVisitor;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;

/**
 * Primitive long version of collection that with given a sequence of add and removal operations, tracks
 * which elements need to actually be added and removed at minimum from some
 * target collection such that the result is equivalent to just
 * executing the sequence of additions and removals in order.
 *
 * @param <T> type of augmented elements iterator
 */
public class PrimitiveLongDiffSets<T extends PrimitiveLongIterator> implements PrimitiveLongReadableDiffSets
{
    private PrimitiveLongSet addedElements;
    private PrimitiveLongSet removedElements;

    public PrimitiveLongDiffSets()
    {
        this( emptySet(), emptySet() );
    }

    public PrimitiveLongDiffSets( PrimitiveLongSet addedElements, PrimitiveLongSet removedElements )
    {
        this.addedElements = addedElements;
        this.removedElements = removedElements;
    }

    @Override
    public boolean isAdded( long element )
    {
        return addedElements.contains( element );
    }

    @Override
    public boolean isRemoved( long element )
    {
        return removedElements.contains( element );
    }

    public void add( long element )
    {
        checkAddedElements();
        boolean removed = removedElements.remove( element );
        if ( !removed )
        {
            addedElements.add( element );
        }
    }

    public boolean remove( long element )
    {
        checkRemovedElements();
        boolean removed = addedElements.remove( element );
        return removed || removedElements.add( element );
    }

    public void accept( PrimitiveLongDiffSetsVisitor visitor )
            throws ConstraintValidationException, CreateConstraintFailureException
    {
        PrimitiveLongIterator addedItems = addedElements.iterator();
        while ( addedItems.hasNext() )
        {
            visitor.visitAdded( addedItems.next() );
        }
        PrimitiveLongIterator removedItems = removedElements.iterator();
        while ( removedItems.hasNext() )
        {
            visitor.visitRemoved( removedItems.next() );
        }
    }

    public T augment( T source )
    {
        return (T) new DiffApplyingPrimitiveLongIterator( source, addedElements, removedElements );
    }

    public PrimitiveLongIterator augmentWithRemovals( PrimitiveLongIterator source )
    {
        return new DiffApplyingPrimitiveLongIterator( source, PrimitiveLongCollections.emptySet(), removedElements );
    }

    @Override
    public int delta()
    {
        return addedElements.size() - removedElements.size();
    }

    @Override
    public PrimitiveLongSet getAdded()
    {
        return addedElements;
    }

    @Override
    public PrimitiveLongSet getRemoved()
    {
        return removedElements;
    }

    @Override
    public PrimitiveLongSet getAddedSnapshot()
    {
        return PrimitiveLongCollections.asSet( addedElements );
    }

    @Override
    public boolean isEmpty()
    {
        return addedElements.isEmpty() && removedElements.isEmpty();
    }

    private void checkAddedElements()
    {
        if ( emptySet() == addedElements )
        {
            addedElements = Primitive.longSet();
        }
    }

    private void checkRemovedElements()
    {
        if ( emptySet() == removedElements )
        {
            removedElements = Primitive.longSet();
        }
    }

}
