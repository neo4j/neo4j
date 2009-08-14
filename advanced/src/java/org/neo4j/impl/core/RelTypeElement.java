/* Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.impl.util.IntArray;

class RelTypeElement implements RelTypeElementIterator
{
    private final IntArray src;
    private final IntArray add;
    private final Set<Integer> remove = new HashSet<Integer>();
    
    private boolean srcTraversed = false;
    private boolean addTraversed = false;
    private int position = 0;
    private Integer nextElement = null;
    
    static RelTypeElementIterator create( IntArray src, IntArray add, 
        IntArray remove )
    {
        if ( add == null && remove == null )
        {
            return new FastRelTypeElement( src );
        }
        return new RelTypeElement( src, add, remove );
    }
    
    private RelTypeElement( IntArray src, IntArray add, IntArray remove )
    {
        this.src = src;
        if ( src == null )
        {
            srcTraversed = true;
        }
        if ( add == null )
        {
            addTraversed = true;
        }
        this.add = add;
        if ( remove != null )
        {
            for ( int i = 0; i < remove.length(); i++ )
            {
                this.remove.add( remove.get( i ) );
            }
        }
    }

    public boolean hasNext()
    {
        if ( nextElement != null )
        {
            return true;
        }
        while ( !srcTraversed && position < src.length() )
        {
            int value = src.get( position++ );
            if ( position >= src.length() )
            {
                srcTraversed = true;
                position = 0;
            }
            if ( !remove.contains( value ) )
            {
                nextElement = value;
                return true;
            }
        }
        while ( !addTraversed && position < add.length() )
        {
            int value = add.get( position++ );
            if ( position >= add.length() )
            {
                addTraversed = true;
                position = 0;
            }
            if ( !remove.contains( value ) )
            {
                nextElement = value;
                return true;
            }
        }
        return false;
    }

    public Integer next()
    {
        hasNext();
        if ( nextElement != null )
        {
            Integer elementToReturn = nextElement;
            nextElement = null;
            return elementToReturn;
        }
        throw new NoSuchElementException();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
