/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.values.virtual;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.neo4j.values.AnyValue;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.Value;

/**
 * This class is way too similar to org.neo4j.collection.primitive.PrimitiveArrays.
 * <p>
 * Should we introduce dependency on primitive collections?
 */
final class ArrayHelpers
{
    private ArrayHelpers()
    {
    }

    static boolean isSortedSet( int[] keys )
    {
        for ( int i = 0; i < keys.length - 1; i++ )
        {
            if ( keys[i] >= keys[i + 1] )
            {
                return false;
            }
        }
        return true;
    }

    static boolean isSortedSet( VirtualValue[] keys, Comparator<AnyValue> comparator )
    {
        for ( int i = 0; i < keys.length - 1; i++ )
        {
            if ( comparator.compare( keys[i], keys[i + 1] ) >= 0 )
            {
                return false;
            }
        }
        return true;
    }

    static boolean isSortedSet( Value[] keys, Comparator<AnyValue> comparator )
    {
        for ( int i = 0; i < keys.length - 1; i++ )
        {
            if ( comparator.compare( keys[i], keys[i + 1] ) >= 0 )
            {
                return false;
            }
        }
        return true;
    }

    static boolean containsNull( AnyValue[] values )
    {
        for ( AnyValue value : values )
        {
            if ( value == null )
            {
                return true;
            }
        }
        return false;
    }

    static boolean containsNull( List<AnyValue> values )
    {
        for ( AnyValue value : values )
        {
            if ( value == null )
            {
                return true;
            }
        }
        return false;
    }

    static <T> Iterator<T> asIterator( T[] array )
    {
        assert array != null;
        return new Iterator<T>()
        {
            private int index;

            @Override
            public boolean hasNext()
            {
                return index < array.length;
            }

            @Override
            public T next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                return array[index++];
            }
        };
    }
}
