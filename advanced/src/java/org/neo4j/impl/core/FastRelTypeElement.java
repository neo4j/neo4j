/* Copyright (c) 2002-2008 "Neo Technology,"
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

import java.util.NoSuchElementException;

import org.neo4j.impl.util.IntArray;

class FastRelTypeElement implements RelTypeElementIterator
{
    private final IntArray src;

    private int position = 0;
    private Integer nextElement = null;
    
    FastRelTypeElement( IntArray src )
    {
        if ( src == null )
        {
            this.src = new IntArray();
        }
        else
        {
            this.src = src;
        }
    }

    public boolean hasNext()
    {
        if ( nextElement != null )
        {
            return true;
        }
        while ( position < src.length() )
        {
            nextElement = src.get(position++);
            return true;
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
