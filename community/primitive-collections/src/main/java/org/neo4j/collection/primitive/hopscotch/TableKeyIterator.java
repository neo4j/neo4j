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
package org.neo4j.collection.primitive.hopscotch;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;

public class TableKeyIterator<VALUE> extends PrimitiveLongBaseIterator
{
    private final Table<VALUE> stable;
    private final long nullKey;
    private final int max;
    private int i;

    public TableKeyIterator( Table<VALUE> table )
    {
        this.stable = table;
        this.nullKey = stable.nullKey();
        this.max = stable.capacity();
    }

    protected boolean isVisible( int index, long key )
    {
        return key != nullKey;
    }

    @Override
    protected boolean fetchNext()
    {
        while ( i < max )
        {
            int index = i++;
            long key = stable.key( index );
            if ( isVisible( index, key ) )
            {
                return next( key );
            }
        }
        return false;
    }
}
