/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;
import java.util.Set;

public final class DiffApplyingPrimitiveLongIterator extends AbstractPrimitiveLongIterator
{
    private enum Phase
    {

        FILTERED_SOURCE
        {
            @Override
            void computeNext( DiffApplyingPrimitiveLongIterator self )
            {
                self.computeNextFromSourceAndFilter();
            }
        },

        ADDED_ELEMENTS
        {
            @Override
            void computeNext( DiffApplyingPrimitiveLongIterator self )
            {
                self.computeNextFromAddedElements();
            }
        },

        NO_ADDED_ELEMENTS
        {
            @Override
            void computeNext( DiffApplyingPrimitiveLongIterator self )
            {
                self.hasNext = false;
            }
        };

        abstract void computeNext( DiffApplyingPrimitiveLongIterator self );
    }

    private final PrimitiveLongIterator source;
    private final Iterator<?> addedElementsIterator;
    private final Set<?> addedElements;
    private final Set<?> removedElements;

    Phase phase;

    public DiffApplyingPrimitiveLongIterator( PrimitiveLongIterator source,
                                              Set<?> addedElements, Set<?> removedElements )
    {
        this.source = source;
        this.addedElements = addedElements;
        this.addedElementsIterator = addedElements == null ? null : addedElements.iterator();
        this.removedElements = removedElements;
        phase = Phase.FILTERED_SOURCE;

        computeNext();
    }

    @Override
    protected void computeNext()
    {
        phase.computeNext( this );
    }

    private void computeNextFromSourceAndFilter()
    {
        for ( hasNext = source.hasNext(); hasNext; hasNext = source.hasNext() )
        {
            nextValue = source.next();
            if ( ( removedElements == null || !removedElements.contains( nextValue ) ) &&
                 ( addedElements == null || !addedElements.contains( nextValue ) ) )
            {
                return;
            }
        }

        transitionToAddedElements();
    }

    private void transitionToAddedElements()
    {
        phase = addedElementsIterator == null ? Phase.NO_ADDED_ELEMENTS : Phase.ADDED_ELEMENTS;
        computeNext();
    }

    private void computeNextFromAddedElements()
    {
        hasNext = addedElementsIterator.hasNext();
        if ( hasNext )
        {
            nextValue = (Long) addedElementsIterator.next();
        }
    }
}
