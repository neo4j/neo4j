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

import java.util.concurrent.Future;

import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;

import static org.neo4j.helpers.FutureAdapter.VOID;

public class RecoveringIndexProxy extends AbstractSwallowingIndexProxy
{
    public RecoveringIndexProxy( IndexDescriptor descriptor, SchemaIndexProvider.Descriptor providerDescriptor )
    {
        super( descriptor, providerDescriptor, null );
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.POPULATING;
    }

    @Override
    public boolean awaitStoreScanCompleted() throws IndexPopulationFailedKernelException, InterruptedException
    {
        throw new UnsupportedOperationException( "cannot await population on a recovering index" );
    }

    @Override
    public void activate()
    {
        throw new UnsupportedOperationException( "Cannot activate recovering index." );
    }

    @Override
    public void validate()
    {
        throw new UnsupportedOperationException( "Cannot validate recovering index." );
    }

    @Override
    public Future<Void> drop()
    {
        return VOID;
    }
}
