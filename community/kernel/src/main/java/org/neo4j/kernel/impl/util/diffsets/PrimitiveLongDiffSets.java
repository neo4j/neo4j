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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.io.Closeable;
import java.util.Objects;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.OnHeapCollectionsFactory;
import org.neo4j.storageengine.api.txstate.PrimitiveLongDiffSetsVisitor;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;

/**
 * Primitive long version of collection that with given a sequence of add and removal operations, tracks
 * which elements need to actually be added and removed at minimum from some
 * target collection such that the result is equivalent to just
 * executing the sequence of additions and removals in order.
 */
public class PrimitiveLongDiffSets implements PrimitiveLongReadableDiffSets, Closeable
{
    private static final MutableLongSet NOT_INITIALIZED = LongSets.mutable.empty();

    private final CollectionsFactory collectionsFactory;
    private MutableLongSet addedElements;
    private MutableLongSet removedElements;

    public PrimitiveLongDiffSets()
    {
        this( NOT_INITIALIZED, NOT_INITIALIZED, OnHeapCollectionsFactory.INSTANCE );
    }

    public PrimitiveLongDiffSets( MutableLongSet addedElements, MutableLongSet removedElements, CollectionsFactory collectionsFactory )
    {
        this.addedElements = addedElements;
        this.removedElements = removedElements;
        this.collectionsFactory = collectionsFactory;
    }

    public PrimitiveLongDiffSets( CollectionsFactory collectionsFactory )
    {
        this( NOT_INITIALIZED, NOT_INITIALIZED, collectionsFactory );
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

    public void removeAll( LongIterator elementsToRemove )
    {
        checkRemovedElements();
        while ( elementsToRemove.hasNext() )
        {
            removeElement( elementsToRemove.next() );
        }
    }

    public void addAll( LongIterator elementsToAdd )
    {
        checkAddedElements();
        while ( elementsToAdd.hasNext() )
        {
            addElement( elementsToAdd.next() );
        }
    }

    public void add( long element )
    {
        checkAddedElements();
        addElement( element );
    }

    public boolean remove( long element )
    {
        checkRemovedElements();
        return removeElement( element );
    }

    public void visit( PrimitiveLongDiffSetsVisitor visitor )
    {
        LongIterator addedItems = addedElements.longIterator();
        while ( addedItems.hasNext() )
        {
            visitor.visitAdded( addedItems.next() );
        }
        LongIterator removedItems = removedElements.longIterator();
        while ( removedItems.hasNext() )
        {
            visitor.visitRemoved( removedItems.next() );
        }
    }

    @Override
    public LongIterator augment( LongIterator source )
    {
        return DiffApplyingPrimitiveLongIterator.augment( source, addedElements, removedElements );
    }

    @Override
    public PrimitiveLongResourceIterator augment( PrimitiveLongResourceIterator source )
    {
        return DiffApplyingPrimitiveLongIterator.augment( source, addedElements, removedElements );
    }

    @Override
    public int delta()
    {
        return addedElements.size() - removedElements.size();
    }

    @Override
    public MutableLongSet getAdded()
    {
        return addedElements;
    }

    @Override
    public MutableLongSet getRemoved()
    {
        return removedElements;
    }

    @Override
    public MutableLongSet getAddedSnapshot()
    {
        return addedElements.toSet();
    }

    @Override
    public boolean isEmpty()
    {
        return addedElements.isEmpty() && removedElements.isEmpty();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        PrimitiveLongDiffSets diffSets = (PrimitiveLongDiffSets) o;
        return Objects.equals( addedElements, diffSets.addedElements ) &&
                Objects.equals( removedElements, diffSets.removedElements );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( addedElements, removedElements );
    }

    private void addElement( long element )
    {
        boolean removed = removedElements.remove( element );
        if ( !removed )
        {
            addedElements.add( element );
        }
    }

    private boolean removeElement( long element )
    {
        boolean removed = addedElements.remove( element );
        return removed || removedElements.add( element );
    }

    private void checkAddedElements()
    {
        if ( addedElements == NOT_INITIALIZED )
        {
            addedElements = collectionsFactory.newLongSet();
        }
    }

    private void checkRemovedElements()
    {
        if ( removedElements == NOT_INITIALIZED )
        {
            removedElements = collectionsFactory.newLongSet();
        }
    }

    @Override
    public void close()
    {
        // nop
    }
}
