/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.index;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

/**
 * Combine multiple progressor to act like one single logical progressor seen from client's perspective.
 */
public class BridgingIndexProgressor implements IndexProgressor.EntityValueClient, IndexProgressor
{
    private final EntityValueClient client;
    private final int[] keys;
    // This is a thread-safe queue because it can be used in parallel scenarios.
    // The overhead of a concurrent queue in this case is negligible since typically there will be two or a very few number
    // of progressors and each progressor has many results each
    private final Queue<IndexProgressor> progressors = new ConcurrentLinkedQueue<>();
    private IndexProgressor current;

    public BridgingIndexProgressor( EntityValueClient client, int[] keys )
    {
        this.client = client;
        this.keys = keys;
    }

    @Override
    public boolean next()
    {
        if ( current == null )
        {
            current = progressors.poll();
        }
        while ( current != null )
        {
            if ( current.next() )
            {
                return true;
            }
            else
            {
                current.close();
                current = progressors.poll();
            }
        }
        return false;
    }

    @Override
    public boolean needsValues()
    {
        return client.needsValues();
    }

    @Override
    public void close()
    {
        progressors.forEach( IndexProgressor::close );
    }

    @Override
    public void initialize( IndexDescriptor descriptor,
                            IndexProgressor progressor,
                            IndexQuery[] queries,
                            IndexQueryConstraints constraints,
                            boolean indexIncludesTransactionState )
    {
        assertKeysAlign( descriptor.schema().getPropertyIds() );
        progressors.add( progressor );
    }

    private void assertKeysAlign( int[] keys )
    {
        for ( int i = 0; i < this.keys.length; i++ )
        {
            if ( this.keys[i] != keys[i] )
            {
                throw new UnsupportedOperationException( "Cannot chain multiple progressors with different key set." );
            }
        }
    }

    @Override
    public boolean acceptEntity( long reference, float score, Value... values )
    {
        return client.acceptEntity( reference, score, values );
    }
}
