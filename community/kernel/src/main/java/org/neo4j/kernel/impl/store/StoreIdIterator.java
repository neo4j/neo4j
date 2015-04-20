/*
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
package org.neo4j.kernel.impl.store;

import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

import static java.lang.String.format;

public class StoreIdIterator implements PrimitiveLongIterator
{
    private final RecordStore<?> store;
    private long highId, id;

    public StoreIdIterator( RecordStore<?> store )
    {
        this.store = store;
        this.id = store.getNumberOfReservedLowIds();
    }

    @Override
    public String toString()
    {
        return format( "%s[id=%s/%s; store=%s]", getClass().getSimpleName(), id, highId, store );
    }

    @Override
    public boolean hasNext()
    {
        if ( id < highId )
        {
            return true;
        }
        highId = store.getHighId();
        return id < highId;
    }

    @Override
    public long next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException(
                    format( "ID [%s] has exceeded the high ID [%s] of %s.", id, highId, store ) );
        }
        return id++;
    }
}
