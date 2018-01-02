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
package org.neo4j.kernel.api.impl.index.bitmaps;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

public class LongPageIterator implements PrimitiveLongIterator
{
    private final Iterator<long[]> source;
    private long[] current;
    private int offset;

    public LongPageIterator( Iterator<long[]> source )
    {
        this.source = source;
    }

    @Override
    public boolean hasNext()
    {
        while ( current == null || offset >= current.length )
        {
            if ( !source.hasNext() )
            {
                current = null;
                return false;
            }
            current = source.next();
            offset = 0;
        }
        return true;
    }

    @Override
    public long next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        return current[offset++];
    }
}
