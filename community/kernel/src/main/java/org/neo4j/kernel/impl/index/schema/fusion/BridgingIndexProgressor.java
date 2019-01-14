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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.ArrayDeque;
import java.util.Queue;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;

/**
 * Combine multiple progressor to act like one single logical progressor seen from clients perspective.
 */
public class BridgingIndexProgressor implements IndexProgressor.NodeValueClient, IndexProgressor
{
    private final NodeValueClient client;
    private final int[] keys;
    private final Queue<IndexProgressor> progressors;
    private IndexProgressor current;

    public BridgingIndexProgressor( NodeValueClient client, int[] keys )
    {
        this.client = client;
        this.keys = keys;
        progressors = new ArrayDeque<>();
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
    public void close()
    {
        progressors.forEach( IndexProgressor::close );
    }

    @Override
    public boolean needsValues()
    {
        return client.needsValues();
    }

    @Override
    public void initialize( SchemaIndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] queries )
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
                throw new UnsupportedOperationException( "Can not chain multiple progressors with different key set." );
            }
        }
    }

    @Override
    public boolean acceptNode( long reference, Value[] values )
    {
        return client.acceptNode( reference, values );
    }
}
