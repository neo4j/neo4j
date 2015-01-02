/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.index.IndexXaConnection;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnectionHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager;

/**
 * {@link XaConnection} implementation for the Neo4j kernel native store. Contains
 * getter methods for the different stores (node,relationship,property and
 * relationship type).
 * <p>
 * A <CODE>NeoStoreXaConnection</CODE> is obtained from
 * {@link NeoStoreXaDataSource} and then Neo4j persistence layer can perform the
 * operations requested via the store implementations.
 */
public class NeoStoreXaConnection extends XaConnectionHelpImpl
    implements IndexXaConnection // Implements this to enable a temporary workaround, see #createIndex
{
    private final NeoStoreXaResource xaResource;
    private final NeoStore neoStore;

    NeoStoreXaConnection( NeoStore neoStore, XaResourceManager xaRm,
        byte branchId[] )
    {
        super( xaRm );
        this.neoStore = neoStore;

        this.xaResource = new NeoStoreXaResource(
            neoStore.getStorageFileName(), xaRm, branchId );
    }

    @Override
    public XAResource getXaResource()
    {
        return this.xaResource;
    }
    
    @Override
    public NeoStoreTransaction getTransaction()
    {
        try
        {
            return (NeoStoreTransaction) super.getTransaction();
        }
        catch ( XAException e )
        {
            throw new TransactionFailureException( "Unable to create transaction.", e );
        }
    }
    
    @Override
    public NeoStoreTransaction createTransaction()
    {
        // Is only called once per write transaction so no need
        // to cache the transaction here.
        try
        {
            return (NeoStoreTransaction) super.createTransaction();
        }
        catch ( XAException e )
        {
            throw new TransactionFailureException( "Unable to create transaction.", e );
        }
    }

    private static class NeoStoreXaResource extends XaResourceHelpImpl
    {
        private final Object identifier;

        NeoStoreXaResource( Object identifier, XaResourceManager xaRm,
            byte branchId[] )
        {
            super( xaRm, branchId );
            this.identifier = identifier;
        }

        @Override
        public boolean isSameRM( XAResource xares )
        {
            if ( xares instanceof NeoStoreXaResource )
            {
                return identifier
                    .equals( ((NeoStoreXaResource) xares).identifier );
            }
            return false;
        }
    };

    // TEST These methods are only used by tests - refactor away if possible
    public PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    public RelationshipTypeTokenStore getRelationshipTypeStore()
    {
        return neoStore.getRelationshipTypeStore();
    }

    @Override
    public void createIndex( Class<? extends PropertyContainer> entityType, String indexName,
            Map<String, String> config )
    {
        // This gets called in the index creator thread in IndexManagerImpl when "creating"
        // an index which uses the graph as its backing. Normally this would add a command to
        // a log, put the transaction in a non-read-only state and cause it to commit and
        // write these command plus add it to the index store (where index configuration is kept).
        // But this is a temporary workaround for supporting in-graph indexes without the
        // persistence around their creation or existence. The reason is that there are no
        // index life cycle commands for the neo store. When/if graph data source gets merged
        // with other index data sources (i.e. they will have one unified log and data source
        // to act as the front end) this will be resolved and this workaround can be removed
        // (NeoStoreXaConnection implementing IndexXaConnection).
    }
}