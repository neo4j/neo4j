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

public class PrimitiveLongIteratorForArray implements PrimitiveLongIterator
{
    private final long[] values;

    int i = 0;

    public PrimitiveLongIteratorForArray( long... values )
    {
        this.values = values;
    }

    public PrimitiveLongIteratorForArray( Long[] values )
    {
        this( unboxedLongs( values ) );
    }

    private static long[] unboxedLongs( Long[] values )
    {
        long[] result = new long[ values.length ];
        for ( int i = 0; i < result.length; i++ )
        {
            Long value = values[i];
            if ( null == value )
            {
                throw new IllegalArgumentException( "Cannot unbox Long[] containing null at position " + i );
            }
            else
            {
                result[i] = value;
            }
        }
        return result;
    }

    @Override
    public boolean hasNext()
    {
        return i < values.length;
    }

    @Override
    public long next()
    {
        if ( hasNext() )
        {
            return values[i++];
        }
        else
        {
            throw new NoSuchElementException( );
        }
    }
}
