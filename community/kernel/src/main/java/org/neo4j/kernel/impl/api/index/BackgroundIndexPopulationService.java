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
import static org.neo4j.helpers.collection.IteratorUtil.single;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.IndexPopulatorMapper;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class BackgroundIndexPopulationService implements IndexPopulationService
{
    private final ExecutorService populationExecutor = newFixedThreadPool( 3 ); // TODO
    private final IndexPopulatorMapper indexManipulatorMapper;
    private final StatementContext readContext;
    private final NeoStore neoStore;
    
    // TODO such synchronization needed?
    private final Collection<IndexPopulationJob> indexJobs = new CopyOnWriteArrayList<IndexPopulationJob>();
    private final ThreadToStatementContextBridge ctxProvider;

    public BackgroundIndexPopulationService( IndexPopulatorMapper indexManipulatorMapper,
                                             NeoStore neoStore,
                                             ThreadToStatementContextBridge ctxProvider )
    {
        this.indexManipulatorMapper = indexManipulatorMapper;
        this.neoStore = neoStore;
        this.ctxProvider = ctxProvider;
        this.readContext = ctxProvider.getCtxForReading();
    }
    
    @Override
    public void indexCreated( IndexDefinition index )
    {
        String propertyKey = single( index.getPropertyKeys() );
        long labelId;
        long propertyKeyId;
        try
        {
            labelId = readContext.getLabelId( index.getLabel().name() );
            propertyKeyId = readContext.getPropertyKeyId( propertyKey );
        }
        catch ( LabelNotFoundKernelException e )
        {
            throw new ThisShouldNotHappenError( "Mattias", "Label " + index.getLabel() + " should exist" );
        }
        catch ( PropertyKeyNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Mattias", "Property " + propertyKey + " should exist" );
        }
        
        // TODO task management
        IndexPopulator populator = indexManipulatorMapper.getPopulator( index );
        IndexPopulationJob job = new IndexPopulationJob( labelId, propertyKeyId, populator, neoStore, ctxProvider );
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
