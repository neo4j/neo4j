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
package org.neo4j.kernel.impl.util;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntCollections.PrimitiveIntBaseIterator;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphdb.Resource;

/**
 * Please dedup with {@link DiffApplyingPrimitiveLongIterator}
 * Applies a diffset to the given source PrimitiveIntIterator.
 * If the given source is a Resource, then so is this DiffApplyingPrimitiveIntIterator.
 */
public final class DiffApplyingPrimitiveIntIterator extends PrimitiveIntBaseIterator implements Resource
{
    private enum Phase
    {
        FILTERED_SOURCE
        {
            @Override
            boolean fetchNext( DiffApplyingPrimitiveIntIterator self )
            {
                return self.computeNextFromSourceAndFilter();
            }
        },

        ADDED_ELEMENTS
        {
            @Override
            boolean fetchNext( DiffApplyingPrimitiveIntIterator self )
            {
                return self.computeNextFromAddedElements();
            }
        },

        NO_ADDED_ELEMENTS
        {
            @Override
            boolean fetchNext( DiffApplyingPrimitiveIntIterator self )
            {
                return false;
            }
        };

        abstract boolean fetchNext( DiffApplyingPrimitiveIntIterator self );
    }

    private final PrimitiveIntIterator source;
    private final Iterator<?> addedElementsIterator;
    private final Set<?> addedElements;
    private final Set<?> removedElements;
    private Phase phase;

    public DiffApplyingPrimitiveIntIterator( PrimitiveIntIterator source,
                                              Set<?> addedElements, Set<?> removedElements )
    {
        this.source = source;
        this.addedElements = addedElements;
        this.addedElementsIterator = addedElements.iterator();
        this.removedElements = removedElements;
        phase = Phase.FILTERED_SOURCE;
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
            int value = source.next();
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
        phase = !addedElementsIterator.hasNext() ? Phase.NO_ADDED_ELEMENTS : Phase.ADDED_ELEMENTS;
    }

    private boolean computeNextFromAddedElements()
    {
        return addedElementsIterator.hasNext() ? next( (Integer) addedElementsIterator.next() ) : false;
    }

    @Override
    public void close()
    {
        if ( source instanceof Resource )
        {
            ((Resource) source).close();
        }
    }
}
