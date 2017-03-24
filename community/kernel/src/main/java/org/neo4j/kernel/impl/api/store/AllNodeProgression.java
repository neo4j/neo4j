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

import org.neo4j.kernel.impl.store.NodeStore;

public class AllNodeProgression implements NodeProgression
{
    private final NodeStore nodeStore;
    private long start;

    AllNodeProgression( NodeStore nodeStore )
    {
        this.nodeStore = nodeStore;
        this.start = nodeStore.getNumberOfReservedLowIds();
    }

    @Override
    public boolean nextBatch( Batch batch )
    {
        long highId = nodeStore.getHighId();
        if ( start > highId )
        {
            batch.nothing();
            return false;
        }
        batch.init( start, highId );
        start = highId + 1;
        return true;
    }

    @Override
    public TransactionStateAccessMode mode()
    {
        return TransactionStateAccessMode.APPEND;
    }
}
