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

import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexStoreView;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.logging.Logging;

public class PopulatingIndexProxy implements IndexProxy
{
    private final JobScheduler scheduler;
    private final IndexDescriptor descriptor;
    private final IndexPopulationJob job;

    public PopulatingIndexProxy( JobScheduler scheduler, IndexDescriptor descriptor, IndexPopulator writer,
                                   FlippableIndexProxy flipper, IndexStoreView storeView, Logging logging )
    {
        this.scheduler  = scheduler;
        this.descriptor = descriptor;
        this.job        = new IndexPopulationJob( descriptor, writer, flipper, storeView, logging );
    }

    @Override
    public void start()
    {
        scheduler.submit( job );
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        job.update( updates );
    }

    @Override
    public Future<Void> drop()
    {
        return job.cancel();
    }

    @Override
    public IndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.POPULATING;
    }
    
    @Override
    public void force()
    {
        // Ignored... I think
    }

    @Override
    public Future<Void> close()
    {
        return job.cancel();
    }
    
    @Override
    public IndexReader newReader() throws IndexNotFoundKernelException
    {
        throw new IndexNotFoundKernelException( descriptor + " is still populating" );
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[job:" + job + "]";
    }
}
