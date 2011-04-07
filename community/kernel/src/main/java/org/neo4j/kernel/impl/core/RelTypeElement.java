/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.RelIdIterator;

class RelTypeElement extends RelTypeElementIterator
{
    private final RelIdArray src;
    private final Set<Long> remove = new HashSet<Long>();
    private final RelIdIterator srcIterator;
    private final RelIdIterator addIterator;
    private RelIdIterator currentIterator;
    private Long nextElement = null;

    static RelTypeElementIterator create( String type, NodeImpl node,
            RelIdArray src, RelIdArray add, RelIdArray remove )
    {
        if ( add == null && remove == null )
        {
            return new FastRelTypeElement( type, node, src );
        }
        return new RelTypeElement( type, node, src, add, remove );
    }

    private RelTypeElement( String type, NodeImpl node, RelIdArray src,
            RelIdArray add, RelIdArray remove )
    {
        super( type, node );
        if ( src == null )
        {
            src = RelIdArray.EMPTY;
        }
        this.src = src;
        this.srcIterator = src.iterator();
        this.addIterator = add == null ? RelIdArray.EMPTY.iterator() : add.iterator();
        if ( remove != null )
        {
            for ( RelIdIterator iterator = remove.iterator(); iterator.hasNext(); )
            {
                this.remove.add( iterator.next() );
            }
        }
        this.currentIterator = srcIterator;
    }

    public boolean hasNext( NodeManager nodeManager )
    {
        if ( nextElement != null )
        {
            return true;
        }
        
        while ( currentIterator.hasNext() || currentIterator == srcIterator )
        {
            while ( currentIterator.hasNext() )
            {
                long value = currentIterator.next();
                if ( !remove.contains( value ) )
                {
                    nextElement = value;
                    return true;
                }
            }
            currentIterator = addIterator;
        }
        return false;
    }

    public long next( NodeManager nodeManager )
    {
        hasNext( nodeManager );
        if ( nextElement != null )
        {
            Long elementToReturn = nextElement;
            nextElement = null;
            return elementToReturn;
        }
        throw new NoSuchElementException();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
    
    public boolean isSrcEmpty()
    {
        return src.isEmpty();
    }
    
    @Override
    public RelTypeElementIterator setSrc( RelIdArray newSrc )
    {
        return new FastRelTypeElement( getType(), getNode(), newSrc, srcIterator.position() );
    }
}
