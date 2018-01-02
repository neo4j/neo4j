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
package org.neo4j.helpers.collection;

/**
 * Iterates over a range, where the start value is inclusive, but the
 * end value is exclusive.
 */
public class RangeIterator extends PrefetchingIterator<Integer>
{
    private int current;
    private final int end;
    private final int stride;
    
    public RangeIterator( int end )
    {
        this( 0, end );
    }
    
    public RangeIterator( int start, int end )
    {
        this( start, end, 1 );
    }
    
    public RangeIterator( int start, int end, int stride )
    {
        this.current = start;
        this.end = end;
        this.stride = stride;
    }
    
    @Override
    protected Integer fetchNextOrNull()
    {
        try
        {
            return current < end ? current : null;
        }
        finally
        {
            current += stride;
        }
    }
}
