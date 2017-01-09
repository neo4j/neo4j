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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.cursor.Cursor;
import org.neo4j.storageengine.api.DegreeItem;

class DegreeItemCursor implements Cursor<DegreeItem>, DegreeItem
{
    private final PrimitiveIntObjectMap<int[]> degrees;
    private PrimitiveIntIterator keys;

    private int type;
    private int outgoing;
    private int incoming;

    DegreeItemCursor( PrimitiveIntObjectMap<int[]> degrees )
    {
        this.keys = degrees.iterator();
        this.degrees = degrees;
    }

    @Override
    public void close()
    {
        keys = null;
    }

    @Override
    public int type()
    {
        return type;
    }

    @Override
    public long outgoing()
    {
        return outgoing;
    }

    @Override
    public long incoming()
    {
        return incoming;
    }

    @Override
    public DegreeItem get()
    {
        if ( keys == null )
        {
            throw new IllegalStateException( "No next item found" );
        }
        return this;
    }

    @Override
    public boolean next()
    {
        if ( keys != null && keys.hasNext() )
        {
            type = keys.next();
            int[] degreeValues = degrees.get( type );
            outgoing = degreeValues[0] + degreeValues[2];
            incoming = degreeValues[1] + degreeValues[2];

            return true;
        }
        keys = null;
        return false;
    }
}
