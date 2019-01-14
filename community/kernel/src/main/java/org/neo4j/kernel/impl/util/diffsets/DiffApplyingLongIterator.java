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

import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nullable;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;

/**
 * Applies a diffset to the given source PrimitiveLongIterator.
 * If the given source is a Resource, then so is this DiffApplyingPrimitiveLongIterator.
 */

class DiffApplyingLongIterator extends PrimitiveLongBaseIterator implements PrimitiveLongResourceIterator
{
    protected enum Phase
    {
        FILTERED_SOURCE
        {
            @Override
            boolean fetchNext( DiffApplyingLongIterator self )
            {
                return self.computeNextFromSourceAndFilter();
            }
        },

        ADDED_ELEMENTS
        {
            @Override
            boolean fetchNext( DiffApplyingLongIterator self )
            {
                return self.computeNextFromAddedElements();
            }
        },

        NO_ADDED_ELEMENTS
        {
            @Override
            boolean fetchNext( DiffApplyingLongIterator self )
            {
                return false;
            }
        };

        abstract boolean fetchNext( DiffApplyingLongIterator self );
    }

    private final PrimitiveLongIterator source;
    private final Iterator<?> addedElementsIterator;
    private final Set<?> addedElements;
    private final Set<?> removedElements;
    @Nullable
    private final Resource resource;
    protected Phase phase;

    DiffApplyingLongIterator( PrimitiveLongIterator source, Set<?> addedElements, Set<?> removedElements, @Nullable Resource resource )
    {
        this.source = source;
        this.addedElements = addedElements;
        this.addedElementsIterator = addedElements.iterator();
        this.removedElements = removedElements;
        this.resource = resource;
        this.phase = Phase.FILTERED_SOURCE;
    }

    static PrimitiveLongIterator augment( PrimitiveLongIterator source, Set<?> addedElements, Set<?> removedElements )
    {
        return new DiffApplyingLongIterator( source, addedElements, removedElements, null );
    }

    static PrimitiveLongResourceIterator augment( PrimitiveLongResourceIterator source, Set<?> addedElements, Set<?> removedElements )
    {
        return new DiffApplyingLongIterator( source, addedElements, removedElements, source );
    }

    @Override
    protected boolean fetchNext()
    {
        return phase.fetchNext( this );
    }

    private boolean computeNextFromSourceAndFilter()
    {
        while ( source.hasNext() )
        {
            long value = source.next();
            if ( !removedElements.contains( value ) && !addedElements.contains( value ) )
            {
                return next( value );
            }
        }
        transitionToAddedElements();
        return phase.fetchNext( this );
    }

    private void transitionToAddedElements()
    {
        phase = addedElementsIterator.hasNext() ? Phase.ADDED_ELEMENTS : Phase.NO_ADDED_ELEMENTS;
    }

    private boolean computeNextFromAddedElements()
    {
        return addedElementsIterator.hasNext() && next( (Long) addedElementsIterator.next() );
    }

    @Override
    public void close()
    {
        if ( resource != null )
        {
            resource.close();
        }
    }
}
