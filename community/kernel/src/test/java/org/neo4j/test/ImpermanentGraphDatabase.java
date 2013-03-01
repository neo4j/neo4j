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
package org.neo4j.test;

import static java.util.Arrays.asList;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.FALSE;
import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TRUE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.use_memory_mapped_buffers;
import static org.neo4j.kernel.InternalAbstractGraphDatabase.Configuration.ephemeral;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.InMemoryIndexProvider;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * A database meant to be used in unit tests. It will always be empty on start.
 */
public class ImpermanentGraphDatabase extends EmbeddedGraphDatabase
{
    static final String PATH = "test-data/impermanent-db";

    public ImpermanentGraphDatabase()
    {
        this( new HashMap<String, String>() );
    }

    /*
     * TODO this shouldn't be here. It so happens however that some tests may use the database
     * directory as the path to store stuff and in this case we need to define the path explicitly,
     * otherwise we end up writing outside the workspace and hence leave stuff behind.
     * The other option is to explicitly remove all files present on startup. Either way,
     * the fact that this discussion takes place is indication that things are inconsistent,
     * since an ImpermanentGraphDatabase should not have any mention of a store directory in
     * any case.
     */
    public ImpermanentGraphDatabase( String storeDir )
    {
        this( storeDir, withForcedInMemoryConfiguration( new HashMap<String, String>() ) );
    }

    public ImpermanentGraphDatabase( Map<String, String> params )
    {
        this( PATH, withForcedInMemoryConfiguration( params ) );
    }

    public ImpermanentGraphDatabase( String storeDir, Map<String, String> params )
    {
        this( storeDir, withForcedInMemoryConfiguration( params ),
                Service.load( IndexProvider.class ),
                Iterables.<KernelExtensionFactory<?>, KernelExtensionFactory>cast( Service.load(
                        KernelExtensionFactory.class ) ),
                Service.load( CacheProvider.class ),
                Service.load( TransactionInterceptorProvider.class ),
                asList( (SchemaIndexProvider) new InMemoryIndexProvider() ));
    }
    
    public ImpermanentGraphDatabase( Map<String, String> params, Iterable<IndexProvider> indexProviders,
                                     Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                     Iterable<CacheProvider> cacheProviders,
                                     Iterable<TransactionInterceptorProvider> transactionInterceptorProviders,
                                     Iterable<SchemaIndexProvider> schemaIndexProviders )
    {
        super( PATH, withForcedInMemoryConfiguration( params ), indexProviders, kernelExtensions, cacheProviders,
                transactionInterceptorProviders, schemaIndexProviders );
    }

    public ImpermanentGraphDatabase( String storeDir, Map<String, String> params, Iterable<IndexProvider> indexProviders,
                                        Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                        Iterable<CacheProvider> cacheProviders,
                                        Iterable<TransactionInterceptorProvider> transactionInterceptorProviders,
                                        Iterable<SchemaIndexProvider> schemaIndexProviders )
    {
        super( storeDir, withForcedInMemoryConfiguration( params ), indexProviders, kernelExtensions, cacheProviders,
                transactionInterceptorProviders, schemaIndexProviders );
    }
    
    @Override
    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return life.add( new EphemeralFileSystemAbstraction() );
    }

    private static Map<String, String> withForcedInMemoryConfiguration( Map<String, String> params )
    {
        // Because EphemeralFileChannel doesn't support memorymapping
        Map<String, String> result = new HashMap<String, String>( params );
        result.put( use_memory_mapped_buffers.name(), FALSE );

        // To signal to index provides that we should be in-memory
        result.put( ephemeral.name(), TRUE );
        return result;
    }

    @Override
    protected boolean isEphemeral()
    {
        return true;
    }

    @Override
    protected Logging createLogging()
    {
        return life.add( new SingleLoggingService( StringLogger.loggerDirectory( fileSystem, storeDir ) ) );
    }

    public void cleanContent( boolean retainReferenceNode )
    {
        Transaction tx = beginTx();
        try
        {
            for ( Node node : GlobalGraphOperations.at( this ).getAllNodes() )
            {
                for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
                {
                    rel.delete();
                }
                if ( !node.hasRelationship() )
                {
                    if ( retainReferenceNode )
                    {
                        try
                        {
                            Node referenceNode = getReferenceNode();
                            if ( !node.equals( referenceNode ) )
                            {
                                node.delete();
                            }
                        }
                        catch ( NotFoundException nfe )
                        {
                            // no ref node
                        }
                    }
                    else
                    {
                        node.delete();
                    }
                }
            }
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }
    }

    public void cleanContent()
    {
        cleanContent( false );
    }
}
