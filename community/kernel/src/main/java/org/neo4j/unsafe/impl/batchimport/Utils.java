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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.SourceInputIterator;

/**
 * Common and cross-concern utilities.
 */
public class Utils
{
    public static int safeCastLongToInt( long value )
    {
        if ( value > Integer.MAX_VALUE )
        {
            throw new UnsupportedOperationException( "Not supported a.t.m" );
        }
        return (int) value;
    }

    public static short safeCastLongToShort( long value )
    {
        if ( value > Short.MAX_VALUE )
        {
            throw new UnsupportedOperationException( "Not supported a.t.m" );
        }
        return (short) value;
    }

    public static byte safeCastLongToByte( long value )
    {
        if ( value > Byte.MAX_VALUE )
        {
            throw new UnsupportedOperationException( "Not supported a.t.m" );
        }
        return (byte) value;
    }

    public enum CompareType
    {
        EQ, GT, GE, LT, LE, NE
    };

    public static boolean unsignedCompare( long dataA, long dataB, CompareType compareType )
    {   // works for signed and unsigned values
        switch ( compareType )
        {
        case EQ:
            return (dataA == dataB);
        case GE:
            if ( dataA == dataB )
            {
                return true;
            }
            // fall through to GT
        case GT:
            return !((dataA < dataB) ^ ((dataA < 0) != (dataB < 0)));
        case LE:
            if ( dataA == dataB )
            {
                return true;
            }
            // fall through to LT
        case LT:
            return ((dataA < dataB) ^ ((dataA < 0) != (dataB < 0)));
        case NE:
        }
        return false;
    }

    public static InputIterable<Object> idsOf( final InputIterable<InputNode> nodes )
    {
        return new InputIterable<Object>()
        {
            @Override
            public InputIterator<Object> iterator()
            {
                final InputIterator<InputNode> iterator = nodes.iterator();
                return new SourceInputIterator<Object,InputNode>( iterator )
                {
                    @Override
                    public void close()
                    {
                        iterator.close();
                    }

                    @Override
                    protected Object fetchNextOrNull()
                    {
                        return iterator.hasNext() ? iterator.next().id() : null;
                    }
                };
            }
        };
    }

    private Utils()
    {   // No instances allowed
    }
}
