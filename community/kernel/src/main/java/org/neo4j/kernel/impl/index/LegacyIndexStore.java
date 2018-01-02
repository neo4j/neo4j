/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Function;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.graphdb.index.IndexManager.PROVIDER;

/**
 * Uses an {@link IndexConfigStore} and puts logic around providers and configuration comparison.
 */
public class LegacyIndexStore
{
    private final IndexConfigStore indexStore;
    private final Config config;
    private final Function<String,IndexImplementation> indexProviders;
    private final Supplier<KernelAPI> kernel;

    public LegacyIndexStore( Config config, IndexConfigStore indexStore, Supplier<KernelAPI> kernel,
            Function<String,IndexImplementation> indexProviders )
    {
        this.config = config;
        this.indexStore = indexStore;
        this.kernel = kernel;
        this.indexProviders = indexProviders;
    }

    public Map<String, String> getOrCreateNodeIndexConfig( String indexName, Map<String, String> customConfiguration )
    {
        return getOrCreateIndexConfig( IndexEntityType.Node, indexName, customConfiguration );
    }

    public Map<String, String> getOrCreateRelationshipIndexConfig( String indexName,
            Map<String, String> customConfiguration )
    {
        return getOrCreateIndexConfig( IndexEntityType.Relationship, indexName, customConfiguration );
    }

    private Map<String, String> findIndexConfig(
            Class<? extends PropertyContainer> cls, String indexName,
            Map<String, String> suppliedConfig, Map<?, ?> dbConfig )
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
            return newConfig;
        }

        Map<String, String> configToUse = suppliedConfig;

        // Check db config properties for provider
        String provider;
        IndexImplementation indexProvider;
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
        indexProvider = indexProviders.apply( provider );
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

        return Collections.unmodifiableMap( configToUse );
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
            config = new HashMap<>( config );
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

    private Map<String, String> getOrCreateIndexConfig(
            IndexEntityType entityType, String indexName, Map<String, String> suppliedConfig )
    {
        Map<String,String> config = findIndexConfig(
                entityType.entityClass(), indexName, suppliedConfig, this.config.getParams() );
        if ( !indexStore.has( entityType.entityClass(), indexName ) )
        {   // Ok, we need to create this config
            synchronized ( this )
            {   // Were we the first ones to get here?
                Map<String,String> existing = indexStore.get( entityType.entityClass(), indexName );
                if ( existing != null )
                {
                    // No, someone else made it before us, cool
                    assertConfigMatches(
                            indexProviders.apply( existing.get( PROVIDER ) ), indexName, existing, config );
                    return config;
                }

                // We were the first one here, let's create this config
                try ( KernelTransaction transaction = kernel.get().newTransaction();
                      Statement statement = transaction.acquireStatement() )
                {
                    switch ( entityType )
                    {
                    case Node:
                        statement.dataWriteOperations().nodeLegacyIndexCreate( indexName, config );
                        break;

                    case Relationship:
                        statement.dataWriteOperations().relationshipLegacyIndexCreate( indexName, config );
                        break;
                    }

                    transaction.success();
                }
                catch ( Exception ex )
                {
                    throw new TransactionFailureException(
                            "Index creation failed for " + indexName + ", " + config, ex );
                }
            }
        }
        return config;
    }

    public String setNodeIndexConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException
    {
        assertLegalConfigKey( key );
        Map<String, String> config = new HashMap<>( getNodeIndexConfiguration( indexName ) );
        String oldValue = config.put( key, value );
        indexStore.set( Node.class, indexName, config );
        return oldValue;
    }

    public String setRelationshipIndexConfiguration( String indexName, String key, String value )
            throws LegacyIndexNotFoundKernelException
    {
        assertLegalConfigKey( key );
        Map<String, String> config = new HashMap<>( getRelationshipIndexConfiguration( indexName ) );
        String oldValue = config.put( key, value );
        indexStore.set( Relationship.class, indexName, config );
        return oldValue;
    }

    public String removeNodeIndexConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        assertLegalConfigKey( key );
        Map<String, String> config = new HashMap<>( getNodeIndexConfiguration( indexName ) );
        String value = config.remove( key );
        if ( value != null )
        {
            indexStore.set( Node.class, indexName, config );
        }
        return value;
    }

    public String removeRelationshipIndexConfiguration( String indexName, String key )
            throws LegacyIndexNotFoundKernelException
    {
        assertLegalConfigKey( key );
        Map<String, String> config = new HashMap<>( getRelationshipIndexConfiguration( indexName ) );
        String value = config.remove( key );
        if ( value != null )
        {
            indexStore.set( Relationship.class, indexName, config );
        }
        return value;
    }

    public Map<String, String> getNodeIndexConfiguration( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        Map<String, String> config = indexStore.get( Node.class, indexName );
        if ( config == null )
        {
            throw new LegacyIndexNotFoundKernelException( "No node index '" + indexName + "' found" );
        }
        return config;
    }

    public Map<String, String> getRelationshipIndexConfiguration( String indexName )
            throws LegacyIndexNotFoundKernelException
    {
        Map<String, String> config = indexStore.get( Relationship.class, indexName );
        if ( config == null )
        {
            throw new LegacyIndexNotFoundKernelException( "No relationship index '" + indexName + "' found" );
        }
        return config;
    }

    private void assertLegalConfigKey( String key )
    {
        if ( key.equals( PROVIDER ) )
        {
            throw new IllegalArgumentException( "'" + key + "' cannot be modified" );
        }
    }

    public String[] getAllNodeIndexNames()
    {
        return indexStore.getNames( Node.class );
    }

    public String[] getAllRelationshipIndexNames()
    {
        return indexStore.getNames( Relationship.class );
    }
}
