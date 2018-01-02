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
package org.neo4j.unsafe.impl.batchimport.store;

import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.IdSequence;

/**
 * {@link IdSequence} w/o any synchronization, purely a long incrementing.
 */
public class BatchingIdSequence implements IdSequence
{
    private long nextId = 0;

    public BatchingIdSequence()
    {
        this( 0 );
    }

    public BatchingIdSequence( long startingId )
    {
        nextId = startingId;
    }

    @Override
    public long nextId()
    {
        long result = peek();
        nextId++;
        return result;
    }

    public void reset()
    {
        nextId = 0;
    }

    public void set( long nextId )
    {
        this.nextId = nextId;
    }

    public long peek()
    {
        if ( nextId == IdGeneratorImpl.INTEGER_MINUS_ONE )
        {
            nextId++;
        }
        return nextId;
    }
}
