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
package org.neo4j.kernel.impl.nioneo.xa;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
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

    /**
     * Made public for testing, dont use.
     */
    public PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    CommonAbstractStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    public RelationshipTypeStore getRelationshipTypeStore()
    {
        return neoStore.getRelationshipTypeStore();
    }

    public XAResource getXaResource()
    {
        return this.xaResource;
    }
    
    public WriteTransaction getWriteTransaction()
    {
        // Is only called once per write transaction so no need
        // to cache the transaction here.
        try
        {
            return (WriteTransaction) getTransaction();
        }
        catch ( XAException e )
        {
            throw new TransactionFailureException( 
                "Unable to get transaction.", e );
        }
    }
    
    @Override
    public void destroy()
    {
        super.destroy();
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
}