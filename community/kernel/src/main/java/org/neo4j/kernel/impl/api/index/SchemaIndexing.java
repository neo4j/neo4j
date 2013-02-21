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

import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.IndexPopulatorMapper;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * TODO temporary name
 *
 * Used when committing for notifying schema indexes about updates made to the graph.
 * Currently in the form of {@link PropertyRecord property records}.
 */
public class SchemaIndexing extends LifecycleAdapter
{
    private final XaDataSourceManager dataSourceManager;
    private final ThreadToStatementContextBridge ctxProvider;
    private NeoStore neoStore;
    private IndexPopulationService populationService;
    private final IndexPopulatorMapper populatorMapper;
    
    public SchemaIndexing( XaDataSourceManager dataSourceManager, ThreadToStatementContextBridge ctxProvider,
            IndexPopulatorMapper populatorMapper )
    {
        this.dataSourceManager = dataSourceManager;
        this.ctxProvider = ctxProvider;
        this.populatorMapper = populatorMapper;
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
                    populationService = new BackgroundIndexPopulationService( populatorMapper, neoStore, ctxProvider );
                }
            }
        } );
    }
    
    @Override
    public void stop() throws Throwable
    {
        populationService.shutdown();
        populationService = null;
    }
    
    public void indexUpdates( Iterable<NodePropertyUpdate> updates )
    {
        populationService.indexUpdates( updates );
    }
}
