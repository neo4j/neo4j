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

import static java.util.Collections.newSetFromMap;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * This is the entry point for managing "schema indexes" in the database, including creating,
 * removing and querying such indexes.
 */
public class IndexingService implements Lifecycle, IndexPopulationService
{
    // TODO pull out an interface instead of overriding all methods
    public static final IndexingService NO_INDEXING = new IndexingService( null, null, null )
    {
        @Override
        public void start() throws Throwable
        {
        }
        
        @Override
        public void stop() throws Throwable
        {
        }
        
        @Override
        public void indexUpdates( Iterable<NodePropertyUpdate> updates )
        {
        }
    };
    
    private final XaDataSourceManager dataSourceManager;
    private final ThreadToStatementContextBridge ctxProvider;
    private NeoStore neoStore;
    private IndexPopulationService populationService = NO_POPULATION_SERVICE;
    private IndexPopulationService onlineService = NO_POPULATION_SERVICE;
    private final SchemaIndexProvider provider;
    private final LifeSupport life = new LifeSupport();
    private final Set<Long> populatingIndexes = newSetFromMap( new ConcurrentHashMap<Long, Boolean>() );
    private final StateFlipOver flip = new IndexingServiceFlipOver( populatingIndexes, onlineService );
    
    public IndexingService( XaDataSourceManager dataSourceManager, ThreadToStatementContextBridge ctxProvider,
                            SchemaIndexProvider provider )
    {
        this.dataSourceManager = dataSourceManager;
        this.ctxProvider = ctxProvider;
        this.provider = provider;
    }
    
    @Override
    public void init() throws Throwable
    {
        life.init();
    }
    
    @Override
    public void start() throws Throwable
    {
        dataSourceManager.addDataSourceRegistrationListener( new DataSourceRegistrationListener.Adapter()
        {
            @Override
            public void registeredDataSource( XaDataSource ds )
            {
                if ( ds.getName().equals( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME ) )
                {
                    neoStore = ((NeoStoreXaDataSource)ds).getNeoStore();
                    populationService = life.add(
                            new BackgroundIndexPopulationService( provider, neoStore, ctxProvider, flip ) );
                    onlineService = life.add( new OnlineIndexService( provider ) );
                }
            }
        } );
        life.start();
    }
    
    @Override
    public void stop() throws Throwable
    {
        life.stop();
        populationService = NO_POPULATION_SERVICE;
    }
    
    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }
    
    @Override
    public void indexCreated( IndexRule index )
    {
        populatingIndexes.add( index.getId() );
        populationService.indexCreated( index );
    }
    
    @Override
    public void indexUpdates( Iterable<NodePropertyUpdate> updates )
    {
        // If we're not yet started (i.e. most likely in recovery mode) the population service
        // points to a "no-op" index service. This is fine because we haven't as of yet started
        // any population job so just ignore those.
        populationService.indexUpdates( updates );
        
        onlineService.indexUpdates( updates );
    }
}
