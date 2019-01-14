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
package org.neo4j.internal.kernel.api;

import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Cursor for scanning the property values of nodes in a schema index.
 * <p>
 * Usage pattern:
 * <pre><code>
 *     int nbrOfProps = cursor.numberOfProperties();

 *     Value[] values = new Value[nbrOfProps];
 *     while ( cursor.next() )
 *     {
 *         if ( cursor.hasValue() )
 *         {
 *             for ( int i = 0; i < nbrOfProps; i++ )
 *             {
 *                 values[i] = cursor.propertyValue( i );
 *             }
 *         }
 *         else
 *         {
 *             values[i] = getPropertyValueFromStore( cursor.nodeReference(), cursor.propertyKey( i ) )
 *         }
 *
 *         doWhatYouWantToDoWith( values );
 *     }
 * </code></pre>
 */
public interface NodeValueIndexCursor extends NodeIndexCursor
{
    /**
     * @return the number of properties accessible within the index, and thus from this cursor.
     */
    int numberOfProperties();

    int propertyKey( int offset );

    /**
     * Check before trying to access values with {@link #propertyValue(int)}. Result can change with each call to {@link #next()}.
     *
     * @return {@code true} if {@link #propertyValue(int)} can be used to get property value on cursor's current location,
     * else {@code false}.
     */
    boolean hasValue();

    Value propertyValue( int offset );

    class Empty implements NodeValueIndexCursor
    {

        @Override
        public void node( NodeCursor cursor )
        {

        }

        @Override
        public long nodeReference()
        {
            return -1L;
        }

        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void close()
        {

        }

        @Override
        public boolean isClosed()
        {
            return false;
        }

        @Override
        public int numberOfProperties()
        {
            return 0;
        }

        @Override
        public int propertyKey( int offset )
        {
            return -1;
        }

        @Override
        public boolean hasValue()
        {
            return false;
        }

        @Override
        public Value propertyValue( int offset )
        {
            return NO_VALUE;
        }
    }

    NodeValueIndexCursor EMPTY = new Empty();
}
