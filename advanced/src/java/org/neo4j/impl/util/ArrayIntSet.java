/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.impl.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

// use array for first few properties to decrease memory footprint (and
// to some extent boost performance) for nodes/rels with few properties
public class ArrayIntSet
{
    private int maxRelSize = 256;
    private int[] rels = new int[2];

    // TODO: figure out if we need volatile here?
    private int arrayCount = 0;

    private Set<Integer> relationshipSet = null;

    public boolean add( int id )
    {
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( rels[i] == id )
            {
                return false;
            }
        }
        if ( arrayCount == rels.length && rels.length * 2 <= maxRelSize )
        {
            int newRels[] = new int[rels.length * 2];
            System.arraycopy( rels, 0, newRels, 0, rels.length );
            rels = newRels;
        }
        if ( arrayCount != -1 )
        {
            if ( arrayCount < rels.length )
            {
                rels[arrayCount++] = id;
                return true;
            }
            relationshipSet = new HashSet<Integer>();
            for ( int i = 0; i < arrayCount; i++ )
            {
                relationshipSet.add( rels[i] );
            }
            arrayCount = -1;
        }
        return relationshipSet.add( id );
    }

    public Iterator<Integer> iterator()
    {
        if ( arrayCount == -1 )
        {
            return relationshipSet.iterator();
        }
        return new ArrayIntIterator( rels, arrayCount );
    }

    public boolean remove( int id )
    {
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( rels[i] == id )
            {
                int[] dest = rels;
                if ( arrayCount - 1 < rels.length / 3 )
                {
                    dest = new int[rels.length / 2];
                    System.arraycopy( rels, 0, dest, 0, arrayCount );
                }
                if ( i + 1 < dest.length && (arrayCount - i - 1) > 0 )
                {
                    System.arraycopy( rels, i + 1, dest, i, arrayCount - i - 1 );
                    rels = dest;
                }
                arrayCount--;
                return true;
            }
        }
        if ( arrayCount == -1 )
        {
            return relationshipSet.remove( id );
        }
        return false;
    }

    public Iterable<Integer> values()
    {
        if ( arrayCount == -1 )
        {
            return relationshipSet;
        }
        return new ArrayIntIterator( rels, arrayCount );
    }

    private static class ArrayIntIterator implements Iterator<Integer>,
        Iterable<Integer>
    {
        private int[] intArray;
        private int pos = -1;
        private int arrayCount;

        ArrayIntIterator( int[] array, int count )
        {
            this.intArray = array;
            this.arrayCount = count;
        }

        public boolean hasNext()
        {
            return pos + 1 < arrayCount;
        }

        public Integer next()
        {
            return intArray[++pos];
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public Iterator<Integer> iterator()
        {
            return this;
        }
    }

    public boolean contains( int id )
    {
        for ( int i = 0; i < arrayCount; i++ )
        {
            if ( rels[i] == id )
            {
                return true;
            }
        }
        if ( arrayCount == -1 )
        {
            return relationshipSet.contains( id );
        }
        return false;
    }

    public int size()
    {
        if ( arrayCount != -1 )
        {
            return arrayCount;
        }
        return relationshipSet.size();
    }
}