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

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Resource;

/**
 * Applies a diffset to the provided {@link PrimitiveLongIterator}.
 * If the given source is a {@link Resource}, it will be closed on {@link #close()}.
 */
public class DiffApplyingPrimitiveLongIterator extends PrimitiveLongBaseIterator implements PrimitiveLongResourceIterator
{
    protected enum Phase
    {
        FILTERED_SOURCE
                {
                    @Override
                    boolean fetchNext( DiffApplyingPrimitiveLongIterator self )
                    {
                        return self.computeNextFromSourceAndFilter();
                    }
                },

        ADDED_ELEMENTS
                {
                    @Override
                    boolean fetchNext( DiffApplyingPrimitiveLongIterator self )
                    {
                        return self.computeNextFromAddedElements();
                    }
                },

        NO_ADDED_ELEMENTS
                {
                    @Override
                    boolean fetchNext( DiffApplyingPrimitiveLongIterator self )
                    {
                        return false;
                    }
                };

        abstract boolean fetchNext( DiffApplyingPrimitiveLongIterator self );
    }

    private final PrimitiveLongIterator source;
    private final PrimitiveLongIterator addedElementsIterator;
    private final PrimitiveLongSet addedElements;
    private final PrimitiveLongSet removedElements;
    protected Phase phase;

    DiffApplyingPrimitiveLongIterator( PrimitiveLongIterator source, PrimitiveLongSet addedElements,
            PrimitiveLongSet removedElements )
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
        phase = !addedElementsIterator.hasNext() ? Phase.NO_ADDED_ELEMENTS : Phase.ADDED_ELEMENTS;
    }

    private boolean computeNextFromAddedElements()
    {
        return addedElementsIterator.hasNext() && next( addedElementsIterator.next() );
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
