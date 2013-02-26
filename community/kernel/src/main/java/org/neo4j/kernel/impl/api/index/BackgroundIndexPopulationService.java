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

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class BackgroundIndexPopulationService extends LifecycleAdapter implements IndexPopulationService
{
    private final ExecutorService populationExecutor = newFixedThreadPool( 3 ); // TODO
    private final SchemaIndexProvider indexProvider;
    private final NeoStore neoStore;
    
    // TODO such synchronization needed?
    private final Collection<IndexPopulationJob> indexJobs = new CopyOnWriteArrayList<IndexPopulationJob>();
    private final ThreadToStatementContextBridge ctxProvider;
    private final StateFlipOver flip;

    public BackgroundIndexPopulationService( SchemaIndexProvider indexProvider,
                                             NeoStore neoStore,
                                             ThreadToStatementContextBridge ctxProvider,
                                             StateFlipOver flip )
    {
        this.indexProvider = indexProvider;
        this.neoStore = neoStore;
        this.ctxProvider = ctxProvider;
        this.flip = flip;
    }
    
    @Override
    public void indexCreated( IndexRule index )
    {
        // TODO task management including handling of failures during population.
        IndexWriter populator = indexProvider.getPopulator( index.getId() );
        IndexPopulationJob job = new IndexPopulationJob( index, populator,
                neoStore, ctxProvider, flip, indexJobs );
        populationExecutor.submit( job );
        indexJobs.add( job );
    }
    
    @Override
    public void indexUpdates( Iterable<NodePropertyUpdate> updates )
    {
        for ( IndexPopulationJob job : indexJobs )
            job.indexUpdates( updates );
    }

    @Override
    public void shutdown()
    {
        for ( IndexPopulationJob job : indexJobs )
            job.cancel();
        
        populationExecutor.shutdown();
        try
        {
            if ( !populationExecutor.awaitTermination( 60, SECONDS ) )
                throw new IllegalStateException( "Failed to shut down index service" );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
