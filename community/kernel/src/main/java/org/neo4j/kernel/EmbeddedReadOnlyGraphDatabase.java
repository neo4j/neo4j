/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;

/**
 * A read-only version of {@link EmbeddedGraphDatabase}.
 */
public final class EmbeddedReadOnlyGraphDatabase extends AbstractGraphDatabase
        implements GraphDatabaseService
{
    private static Map<String, String> readOnlyParams = new HashMap<String, String>();

    static
    {
        readOnlyParams.put( Config.READ_ONLY, "true" );
    };

    private final EmbeddedGraphDbImpl graphDbImpl;

    /**
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>. If the directory shouldn't exist or isn't a neo4j
     * store an exception will be thrown.
     *
     * @param storeDir the store directory for the Neo4j store files
     */
    public EmbeddedReadOnlyGraphDatabase( String storeDir )
    {
        this( storeDir, readOnlyParams );
    }

    /**
     * A non-standard way of creating an embedded {@link GraphDatabaseService}
     * with a set of configuration parameters. Will most likely be removed in
     * future releases.
     * <p>
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>. If the directory shouldn't exist or isn't a neo4j
     * store an exception will be thrown.
     *
     * @param storeDir the store directory for the db files
     * @param params configuration parameters
     */
    public EmbeddedReadOnlyGraphDatabase( String storeDir,
            Map<String, String> params )
    {
        super( storeDir );
        params.put( Config.READ_ONLY, "true" );
        caches = createCaches( getMessageLog() );
        this.graphDbImpl = new EmbeddedGraphDbImpl( getStoreDir(), null, params, this,
                CommonFactories.defaultLockManagerFactory(),
                CommonFactories.defaultIdGeneratorFactory(),
                CommonFactories.defaultRelationshipTypeCreator(),
                CommonFactories.defaultTxIdGeneratorFactory(),
                CommonFactories.defaultTxHook(),
                CommonFactories.defaultLastCommittedTxIdSetter(),
                CommonFactories.defaultFileSystemAbstraction(), caches );
    }

    /**
     * A non-standard convenience method that loads a standard property file and
     * converts it into a generic <Code>Map<String,String></CODE>. Will most
     * likely be removed in future releases.
     *
     * @param file the property file to load
     * @return a map containing the properties from the file
     */
    public static Map<String, String> loadConfigurations( String file )
    {
        return EmbeddedGraphDbImpl.loadConfigurations( file );
    }

    public Node createNode()
    {
        return graphDbImpl.createNode();
    }

    public Node getNodeById( long id )
    {
        return graphDbImpl.getNodeById( id );
    }

    public Relationship getRelationshipById( long id )
    {
        return graphDbImpl.getRelationshipById( id );
    }

    public Node getReferenceNode()
    {
        return graphDbImpl.getReferenceNode();
    }

    @Override protected void close()
    {
        graphDbImpl.shutdown();
    }

    @Override
    public TransactionBuilder tx()
    {
        return graphDbImpl.tx();
    }

    /**
     * Returns a non-standard configuration object. Will most likely be removed
     * in future releases.
     *
     * @return a configuration object
     */
    @Override
    public Config getConfig()
    {
        return graphDbImpl.getConfig();
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return graphDbImpl.getManagementBeans( type );
    }

    @Override
    public KernelData getKernelData()
    {
        return graphDbImpl.getKernelData();
    }

    @Override
    public String toString()
    {
        return super.toString() + " [" + graphDbImpl.getStoreDir() + "]";
    }

    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }

    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }

    public IndexManager index()
    {
        return graphDbImpl.index();
    }
}
