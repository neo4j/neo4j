/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.neo4j.cursor.Cursor;

public class Cursors
{
    private static Cursor<Object> EMPTY = new Cursor<Object>()
    {
        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public Object get()
        {
            throw new IllegalStateException( "no elements" );
        }

        @Override
        public void close()
        {
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Cursor<T> empty()
    {
        return (Cursor<T>) EMPTY;
    }

    public static int count( Cursor<?> cursor )
    {
        try
        {
            int count = 0;
            while ( cursor.next() )
            {
                count++;
            }
            return count;
        }
        finally
        {
            cursor.close();
        }
    }
}
