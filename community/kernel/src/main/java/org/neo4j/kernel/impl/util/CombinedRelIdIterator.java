/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.Collection;
import java.util.NoSuchElementException;

import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

public class CombinedRelIdIterator implements RelIdIterator
{
    private RelIdIterator srcIterator;
    private final RelIdIterator addIterator;
    private RelIdIterator currentIterator;
    private final Collection<Long> removed;
    private final int type;
    private boolean nextElementDetermined;
    private long nextElement;
    
    public CombinedRelIdIterator( int type, DirectionWrapper direction, RelIdArray src,
            RelIdArray add, Collection<Long> remove )
    {
        this.type = type;
        this.srcIterator = src != null ? src.iterator( direction ) : RelIdArray.EMPTY.iterator( direction );
        this.addIterator = add != null ? add.iterator( direction ) : RelIdArray.EMPTY.iterator( direction );
        this.currentIterator = srcIterator;
        this.removed = remove;
    }
    
    @Override
    public int getType()
    {
        return type;
    }

    @Override
    public RelIdArray getIds()
    {
        return srcIterator.getIds();
    }

    @Override
    public RelIdIterator updateSource( RelIdArray newSource, DirectionWrapper direction )
    {
        srcIterator = srcIterator.updateSource( newSource, direction );
        return this;
    }

    @Override
    public boolean hasNext()
    {
        if ( nextElementDetermined )
        {
            return nextElement != -1;
        }
        
        while ( currentIterator.hasNext() || currentIterator != addIterator )
        {
            while ( currentIterator.hasNext() )
            {
                long value = currentIterator.next();
                if ( removed == null || !removed.contains( value ) )
                {
                    nextElement = value;
                    nextElementDetermined = true;
                    return true;
                }
            }
            currentIterator = addIterator;
        }
        nextElementDetermined = true;
        nextElement = -1;
        return false;
    }

    @Override
    public void doAnotherRound()
    {
        srcIterator.doAnotherRound();
        addIterator.doAnotherRound();
        nextElementDetermined = false;
        currentIterator = srcIterator;
    }

    @Override
    public long next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        nextElementDetermined = false;
        return nextElement;
    }
}
