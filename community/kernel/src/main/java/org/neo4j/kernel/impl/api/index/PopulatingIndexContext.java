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

import org.neo4j.kernel.api.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexStoreView;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.logging.Logging;

public class PopulatingIndexContext implements IndexContext
{
    private final JobScheduler scheduler;
    private final IndexPopulationJob job;

    public PopulatingIndexContext( JobScheduler scheduler, IndexRule rule, IndexPopulator writer,
                                   FlippableIndexContext flipper, IndexStoreView storeView, Logging logging )
    {
        this.scheduler = scheduler;
        this.job       = new IndexPopulationJob( rule, writer, flipper, storeView, logging );
    }

    @Override
    public void create()
    {
        scheduler.submit( job );
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        job.update( updates );
    }

    @Override
    public void drop()
    {
        job.cancel();
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
}
