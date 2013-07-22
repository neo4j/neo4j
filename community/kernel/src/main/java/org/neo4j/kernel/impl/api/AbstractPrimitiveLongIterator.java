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

import java.util.NoSuchElementException;

/**
 * Subclasses provide a PrimitiveLongIterator by implementing computeNext() which is expected to either
 * set hasNext to false, or to set hasNext to true and to set value to the next value.
 *
 * Before using instances for iteration, computeNext() needs to be called exactly once.  Subclasses are recommended
 * to do this in their constructor.
 */
public abstract class AbstractPrimitiveLongIterator implements PrimitiveLongIterator
{
    protected boolean hasNext = false;
    protected long nextValue = 0l;

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
        else
        {
            throw new NoSuchElementException(  );
        }
    }

    protected abstract void computeNext();
}
