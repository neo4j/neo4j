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
package org.neo4j.kernel.impl.util.diffsets;

import java.io.Closeable;
import java.util.Objects;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.OnHeapCollectionsFactory;
import org.neo4j.storageengine.api.txstate.PrimitiveLongDiffSetsVisitor;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.asSet;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;

/**
 * Primitive long version of collection that with given a sequence of add and removal operations, tracks
 * which elements need to actually be added and removed at minimum from some
 * target collection such that the result is equivalent to just
 * executing the sequence of additions and removals in order.
 */
public class PrimitiveLongDiffSets implements PrimitiveLongReadableDiffSets, Closeable
{
    private final CollectionsFactory collectionsFactory;
    private PrimitiveLongSet addedElements;
    private PrimitiveLongSet removedElements;

    public PrimitiveLongDiffSets()
    {
        this( emptySet(), emptySet(), OnHeapCollectionsFactory.INSTANCE );
    }

    public PrimitiveLongDiffSets( PrimitiveLongSet addedElements, PrimitiveLongSet removedElements, CollectionsFactory collectionsFactory )
    {
        this.addedElements = addedElements;
        this.removedElements = removedElements;
        this.collectionsFactory = collectionsFactory;
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

    public void removeAll( PrimitiveLongIterator elementsToRemove )
    {
        checkRemovedElements();
        while ( elementsToRemove.hasNext() )
        {
            removeElement( elementsToRemove.next() );
        }
    }

    public void addAll( PrimitiveLongIterator elementsToAdd )
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

    @Override
    public PrimitiveLongIterator augment( PrimitiveLongIterator source )
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
        return asSet( addedElements );
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
        if ( emptySet() == addedElements )
        {
            addedElements = collectionsFactory.newLongSet();
        }
    }

    private void checkRemovedElements()
    {
        if ( emptySet() == removedElements )
        {
            removedElements = collectionsFactory.newLongSet();
        }
    }

    @Override
    public void close()
    {
        removedElements.close();
        addedElements.close();
    }
}
