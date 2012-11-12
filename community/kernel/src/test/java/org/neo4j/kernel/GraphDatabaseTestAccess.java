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
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

/**
 * Used in testing and makes some internals configurable, f.ex {@link FileSystemAbstraction}
 * and {@link IdGeneratorFactory}. Otherwise its functionality is equivalent to
 * {@link EmbeddedGraphDatabase}.
 */
public class GraphDatabaseTestAccess extends AbstractGraphDatabase
{
    private final EmbeddedGraphDbImpl impl;
    protected final FileSystemAbstraction fileSystem;

    public GraphDatabaseTestAccess( String storeDir, Map<String, String> config,
            IdGeneratorFactory idGenerators, FileSystemAbstraction fileSystem )
    {
        super( storeDir );
        config = config != null ? config : MapUtil.stringMap();
        impl = new EmbeddedGraphDbImpl( getStoreDir(), null, config, this, CommonFactories.defaultLockManagerFactory(),
                idGenerators, CommonFactories.defaultRelationshipTypeCreator(),
                CommonFactories.defaultTxIdGeneratorFactory(),
                CommonFactories.defaultTxHook(),
                CommonFactories.defaultLastCommittedTxIdSetter(), fileSystem, createCaches( getMessageLog() ) );
        this.fileSystem = fileSystem;
    }

    @Override
    public Node createNode()
    {
        return impl.createNode();
    }

    @Override
    public Node getNodeById( long id )
    {
        return impl.getNodeById( id );
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return impl.getRelationshipById( id );
    }

    @Override
    public Node getReferenceNode()
    {
        return impl.getReferenceNode();
    }

    @Override
    protected void close()
    {
        impl.shutdown();
    }

    @Override
    public TransactionBuilder tx()
    {
        return impl.tx();
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return impl.registerTransactionEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return impl.unregisterTransactionEventHandler( handler );
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return impl.registerKernelEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return impl.unregisterKernelEventHandler( handler );
    }

    @Override
    public IndexManager index()
    {
        return impl.index();
    }

    @Override
    public Config getConfig()
    {
        return impl.getConfig();
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        return impl.getManagementBeans( type );
    }

    @Override
    public KernelData getKernelData()
    {
        return impl.getKernelData();
    }
}
