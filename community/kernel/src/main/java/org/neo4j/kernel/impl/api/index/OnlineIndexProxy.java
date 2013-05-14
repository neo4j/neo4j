/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.concurrent.Future;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;

import static org.neo4j.helpers.FutureAdapter.VOID;

public class OnlineIndexProxy implements IndexProxy
{
    private final IndexDescriptor descriptor;
    final IndexAccessor accessor;
    private final SchemaIndexProvider.Descriptor providerDescriptor;

    public OnlineIndexProxy( IndexDescriptor descriptor, SchemaIndexProvider.Descriptor providerDescriptor,
                             IndexAccessor accessor )
    {
        this.descriptor = descriptor;
        this.providerDescriptor = providerDescriptor;
        this.accessor = accessor;
    }
    
    @Override
    public void start()
    {
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates ) throws IOException
    {
        try
        {
            accessor.updateAndCommit( updates );
        }
        catch ( IndexEntryConflictException e )
        {
            throw e.notAllowed( descriptor );
        }
    }
    
    @Override
    public void recover( Iterable<NodePropertyUpdate> updates ) throws IOException
    {
        accessor.recover( updates );
    }

    @Override
    public Future<Void> drop() throws IOException
    {
        accessor.drop();
        return VOID;
    }

    @Override
    public IndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.ONLINE;
    }
    
    @Override
    public void force() throws IOException
    {
        accessor.force();
    }

    @Override
    public Future<Void> close() throws IOException
    {
        accessor.close();
        return VOID;
    }
    
    @Override
    public IndexReader newReader()
    {
        return accessor.newReader();
    }

    @Override
    public boolean awaitStoreScanCompleted() throws IndexPopulationFailedKernelException, InterruptedException
    {
        return false; // the store scan is already completed
    }

    @Override
    public void activate()
    {
        // ok, already active
    }

    @Override
    public void validate()
    {
        // ok, it's online so it's valid
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[accessor:" + accessor + ", descriptor:" + descriptor + "]";
    }
}
