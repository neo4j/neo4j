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
package org.neo4j.kernel;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.index.IndexXaConnection;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

class IndexManagerImpl implements IndexManager, IndexProviders
{
    private final IndexStore indexStore;
    private final Map<String, IndexImplementation> indexProviders = new HashMap<String, IndexImplementation>();

    private NodeAutoIndexerImpl nodeAutoIndexer;
    private RelationshipAutoIndexerImpl relAutoIndexer;
    private final Config config;
    private final XaDataSourceManager xaDataSourceManager;
    private final AbstractTransactionManager txManager;
    private final GraphDatabaseAPI graphDatabaseAPI;

    IndexManagerImpl( Config config, IndexStore indexStore,
                      XaDataSourceManager xaDataSourceManager, AbstractTransactionManager txManager,
                      GraphDatabaseAPI graphDatabaseAPI
    )
    {
        this.graphDatabaseAPI = graphDatabaseAPI;
        this.config = config;
        this.xaDataSourceManager = xaDataSourceManager;
        this.txManager = txManager;
        this.indexStore = indexStore;
    }

    private IndexImplementation getIndexProvider( String provider )
    {
        if ( provider == null )
        {
            throw new IllegalArgumentException( "No 'provider' given in configuration map" );
        }

        synchronized ( this.indexProviders )
        {
            IndexImplementation result = this.indexProviders.get( provider );
            if ( result != null )
            {
                return result;
            }
            throw new IllegalArgumentException( "No index provider '" + provider +
                    "' found. Maybe the intended provider (or one more of its dependencies) " +
                    "aren't on the classpath or it failed to load." );
        }
    }

    @Override
    public void registerIndexProvider( String name, IndexImplementation provider )
    {
        this.indexProviders.put( name, provider );
    }

    @Override
    public boolean unregisterIndexProvider( String name )
    {
        return this.indexProviders.remove( name ) != null;
    }

    private Pair<Map<String, String>, Boolean/*true=needs to be set*/> findIndexConfig( Class<? extends
            PropertyContainer> cls,
                                                                                        String indexName, Map<String,
            String> suppliedConfig, Map<?, ?> dbConfig )
    {
        // Check stored config (has this index been created previously?)
        Map<String, String> storedConfig = indexStore.get( cls, indexName );
        if ( storedConfig != null && suppliedConfig == null )
        {
            // Fill in "provider" if not already filled in, backwards compatibility issue
            Map<String, String> newConfig = injectDefaultProviderIfMissing( indexName, dbConfig, storedConfig );
            if ( newConfig != storedConfig )
            {
                indexStore.set( cls, indexName, newConfig );
            }
            return Pair.of( newConfig, Boolean.FALSE );
        }

        Map<String, String> configToUse = suppliedConfig;

        // Check db config properties for provider
        String provider = null;
        IndexImplementation indexProvider = null;
        if ( configToUse == null )
        {
            provider = getDefaultProvider( indexName, dbConfig );
            configToUse = MapUtil.stringMap( PROVIDER, provider );
        }
        else
        {
            provider = configToUse.get( PROVIDER );
            provider = provider == null ? getDefaultProvider( indexName, dbConfig ) : provider;
        }
        indexProvider = getIndexProvider( provider );
        configToUse = indexProvider.fillInDefaults( configToUse );
        configToUse = injectDefaultProviderIfMissing( indexName, dbConfig, configToUse );

        // Do they match (stored vs. supplied)?
        if ( storedConfig != null )
        {
            assertConfigMatches( indexProvider, indexName, storedConfig, suppliedConfig );
            // Fill in "provider" if not already filled in, backwards compatibility issue
            Map<String, String> newConfig = injectDefaultProviderIfMissing( indexName, dbConfig, storedConfig );
            if ( newConfig != storedConfig )
            {
                indexStore.set( cls, indexName, newConfig );
            }
            configToUse = newConfig;
        }

        boolean needsToBeSet = !indexStore.has( cls, indexName );
        return Pair.of( Collections.unmodifiableMap( configToUse ), needsToBeSet );
    }

    private void assertConfigMatches( IndexImplementation indexProvider, String indexName,
                                      Map<String, String> storedConfig, Map<String, String> suppliedConfig )
    {
        if ( suppliedConfig != null && !indexProvider.configMatches( storedConfig, suppliedConfig ) )
        {
            throw new IllegalArgumentException( "Supplied index configuration:\n" +
                    suppliedConfig + "\ndoesn't match stored config in a valid way:\n" + storedConfig +
                    "\nfor '" + indexName + "'" );
        }
    }

    private Map<String, String> injectDefaultProviderIfMissing( String indexName, Map<?, ?> dbConfig,
            Map<String, String> config )
    {
        String provider = config.get( PROVIDER );
        if ( provider == null )
        {
            config = new HashMap<String, String>( config );
            config.put( PROVIDER, getDefaultProvider( indexName, dbConfig ) );
        }
        return config;
    }

    private String getDefaultProvider( String indexName, Map<?, ?> dbConfig )
    {
        String provider = null;
        if ( dbConfig != null )
        {
            provider = (String) dbConfig.get( "index." + indexName );
            if ( provider == null )
            {
                provider = (String) dbConfig.get( "index" );
            }
        }

        // 4. Default to lucene
        if ( provider == null )
        {
            provider = "lucene";
        }
        return provider;
    }

    private Pair<Map<String, String>, /*was it created now?*/Boolean> getOrCreateIndexConfig(
            Class<? extends PropertyContainer> cls, String indexName, Map<String, String> suppliedConfig )
    {
        Pair<Map<String, String>, Boolean> result = findIndexConfig( cls,
                indexName, suppliedConfig, config.getParams() );
        boolean createdNow = false;
        if ( result.other() )
        {   // Ok, we need to create this config
            synchronized ( this )
            {   // Were we the first ones to get here?
                Map<String, String> existing = indexStore.get( cls, indexName );
                if ( existing != null )
                {
                    // No, someone else made it before us, cool
                    assertConfigMatches( getIndexProvider( existing.get( PROVIDER ) ), indexName,
                            existing, result.first() );
                    return Pair.of( result.first(), false );
                }

                // We were the first one here, let's create this config
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                try
                {
                    executorService.submit( new IndexCreatorJob( cls, indexName, result.first() ) ).get();
                    indexStore.set( cls, indexName, result.first() );
                    createdNow = true;
                }
                catch ( ExecutionException ex )
                {
                    throw new TransactionFailureException( "Index creation failed for " + indexName +
                            ", " + result.first(), ex.getCause() );
                }
                catch ( InterruptedException ex )
                {
                    Thread.interrupted();
                }
                finally
                {
                    executorService.shutdownNow();
                }
            }
        }
        return Pair.of( result.first(), createdNow );
    }

    private class IndexCreatorJob implements Callable
    {
        private final String indexName;
        private final Map<String, String> config;
        private final Class<? extends PropertyContainer> cls;

        IndexCreatorJob( Class<? extends PropertyContainer> cls, String indexName,
                         Map<String, String> config )
        {
            this.cls = cls;
            this.indexName = indexName;
            this.config = config;
        }

        @Override
        public Object call()
                throws Exception
        {
            String provider = config.get( PROVIDER );
            String dataSourceName = getIndexProvider( provider ).getDataSourceName();
            XaDataSource dataSource = xaDataSourceManager.getXaDataSource( dataSourceName );
            IndexXaConnection connection = (IndexXaConnection) dataSource.getXaConnection();
            try ( Transaction tx = graphDatabaseAPI.tx().begin() )
            {
                javax.transaction.Transaction javaxTx = txManager.getTransaction();
                connection.enlistResource( javaxTx );
                connection.createIndex( cls, indexName, config );
                tx.success();
            }
            return null;
        }
    }

    @Override
    public boolean existsForNodes( String indexName )
    {
        assertInTransaction();
        return indexStore.get( Node.class, indexName ) != null;
    }

    @Override
    public Index<Node> forNodes( String indexName )
    {
        return forNodes( indexName, null );
    }

    @Override
    public Index<Node> forNodes( String indexName,
                                 Map<String, String> customConfiguration )
    {
        assertInTransaction();
        Index<Node> toReturn = getOrCreateNodeIndex( indexName,
                customConfiguration );
        if ( NodeAutoIndexerImpl.NODE_AUTO_INDEX.equals( indexName ) )
        {
            toReturn = new AbstractAutoIndexerImpl.ReadOnlyIndexToIndexAdapter<Node>( toReturn );
        }
        return toReturn;
    }

    Index<Node> getOrCreateNodeIndex(
            String indexName, Map<String, String> customConfiguration )
    {
        Pair<Map<String, String>, Boolean> config = getOrCreateIndexConfig( Node.class,
                indexName, customConfiguration );
        try
        {
            return getIndexProvider( config.first().get( PROVIDER ) ).nodeIndex( indexName,
                    config.first() );
        }
        catch ( RuntimeException e )
        {
            if ( config.other() )
            {
                indexStore.remove( Node.class, indexName );
            }
            throw e;
        }
    }

    RelationshipIndex getOrCreateRelationshipIndex( String indexName,
                                                    Map<String, String> customConfiguration )
    {
        Pair<Map<String, String>, Boolean> config = getOrCreateIndexConfig(
                Relationship.class, indexName, customConfiguration );
        try
        {
            return getIndexProvider( config.first().get( PROVIDER ) ).relationshipIndex(
                    indexName, config.first() );
        }
        catch ( RuntimeException e )
        {
            if ( config.other() )
            {
                indexStore.remove( Relationship.class, indexName );
            }
            throw e;
        }
    }

    @Override
    public String[] nodeIndexNames()
    {
        assertInTransaction();
        return indexStore.getNames( Node.class );
    }

    @Override
    public boolean existsForRelationships( String indexName )
    {
        assertInTransaction();
        return indexStore.get( Relationship.class, indexName ) != null;
    }

    @Override
    public RelationshipIndex forRelationships( String indexName )
    {
        return forRelationships( indexName, null );
    }

    @Override
    public RelationshipIndex forRelationships( String indexName,
                                               Map<String, String> customConfiguration )
    {
        assertInTransaction();
        RelationshipIndex toReturn = getOrCreateRelationshipIndex( indexName,
                customConfiguration );
        if ( RelationshipAutoIndexerImpl.RELATIONSHIP_AUTO_INDEX.equals( indexName ) )
        {
            toReturn = new RelationshipAutoIndexerImpl.RelationshipReadOnlyIndexToIndexAdapter(
                    toReturn );
        }
        return toReturn;
    }

    @Override
    public String[] relationshipIndexNames()
    {
        assertInTransaction();
        return indexStore.getNames( Relationship.class );
    }

    @Override
    public Map<String, String> getConfiguration( Index<? extends PropertyContainer> index )
    {
        Map<String, String> config = indexStore.get( index.getEntityType(), index.getName() );
        if ( config == null )
        {
            throw new NotFoundException( "No " + index.getEntityType().getSimpleName() +
                    " index '" + index.getName() + "' found" );
        }
        return config;
    }

    @Override
    public String setConfiguration( Index<? extends PropertyContainer> index, String key, String value )
    {
        assertLegalConfigKey( key );
        Map<String, String> config = getMutableConfig( index );
        String oldValue = config.put( key, value );
        indexStore.set( index.getEntityType(), index.getName(), config );
        return oldValue;
    }

    private void assertLegalConfigKey( String key )
    {
        if ( key.equals( PROVIDER ) )
        {
            throw new IllegalArgumentException( "'" + key + "' cannot be modified" );
        }
    }

    private Map<String, String> getMutableConfig( Index<? extends PropertyContainer> index )
    {
        return new HashMap<String, String>( getConfiguration( index ) );
    }

    @Override
    public String removeConfiguration( Index<? extends PropertyContainer> index, String key )
    {
        assertLegalConfigKey( key );
        Map<String, String> config = getMutableConfig( index );
        String value = config.remove( key );
        if ( value != null )
        {
            indexStore.set( index.getEntityType(), index.getName(), config );
        }
        return value;
    }

    // TODO These setters/getters stick. Why are these indexers exposed!?
    public void setNodeAutoIndexer( NodeAutoIndexerImpl nodeAutoIndexer )
    {
        this.nodeAutoIndexer = nodeAutoIndexer;
    }

    public void setRelAutoIndexer( RelationshipAutoIndexerImpl relAutoIndexer )
    {
        this.relAutoIndexer = relAutoIndexer;
    }

    @Override
    public AutoIndexer<Node> getNodeAutoIndexer()
    {
        return nodeAutoIndexer;
    }

    @Override
    public RelationshipAutoIndexer getRelationshipAutoIndexer()
    {
        return relAutoIndexer;
    }

    private void assertInTransaction()
    {
        txManager.assertInTransaction();
    }
}
