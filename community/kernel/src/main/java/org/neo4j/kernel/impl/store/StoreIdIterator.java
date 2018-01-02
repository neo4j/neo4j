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
package org.neo4j.kernel.impl.store;

import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

import static java.lang.String.format;

public class StoreIdIterator implements PrimitiveLongIterator
{
    private final RecordStore<?> store;
    private long targetId, id;
    private final boolean forward;

    public StoreIdIterator( RecordStore<?> store )
    {
        this( store, true );
    }

    public StoreIdIterator( RecordStore<?> store, boolean forward )
    {
        this.store = store;
        this.id = forward ? store.getNumberOfReservedLowIds() : store.getHighId();
        this.forward = forward;
    }

    @Override
    public String toString()
    {
        return format( "%s[id=%s/%s; store=%s]", getClass().getSimpleName(), id, targetId, store );
    }

    @Override
    public boolean hasNext()
    {
        if ( forward )
        {
            if ( id < targetId )
            {
                return true;
            }
            targetId = store.getHighId();
            return id < targetId;
        }

        return id > 0;
    }

    @Override
    public long next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException( forward
                    ? format( "ID [%s] has exceeded the high ID [%s] of %s.", id, targetId, store )
                    : format( "ID [%s] has exceeded the low ID [%s] of %s.", id, targetId, store ) );
        }
        try
        {
            return id;
        }
        finally
        {
            id += (forward ? 1 : -1);
        }
    }
}
