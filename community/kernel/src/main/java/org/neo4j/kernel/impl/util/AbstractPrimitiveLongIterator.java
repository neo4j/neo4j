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
package org.neo4j.kernel.impl.util;

import java.util.NoSuchElementException;

import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

/**
 * Subclasses provide a PrimitiveLongIterator by implementing computeNext() which is expected to either
 * set hasNext to false, or to set hasNext to true and to set value to the next value.
 *
 * Before using instances for iteration, computeNext() needs to be called exactly once. Subclasses are recommended
 * to do this in their constructor.
 */
public abstract class AbstractPrimitiveLongIterator implements PrimitiveLongIterator
{
    private boolean hasNext;
    private long nextValue;

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public long next()
    {
        if ( hasNext )
        {
            long result = nextValue;
            computeNext();
            return result;
        }
        
        throw new NoSuchElementException();
    }

    /**
     * Computes the next item in this iterator. Implementations must call either {@link #next(long)}
     * with the computed value, or {@link #endReached()} if there are no more items in this iterator.
     */
    protected abstract void computeNext();
    
    protected void endReached()
    {
        hasNext = false;
    }
    
    protected void next( long value )
    {
        nextValue = value;
        hasNext = true;
    }
}
