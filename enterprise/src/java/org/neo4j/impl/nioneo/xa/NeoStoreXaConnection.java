/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.xa;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.NodeStore;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipStore;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.impl.nioneo.store.StoreFailureException;
import org.neo4j.impl.transaction.xaframework.XaConnection;
import org.neo4j.impl.transaction.xaframework.XaConnectionHelpImpl;
import org.neo4j.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.impl.transaction.xaframework.XaResourceManager;
import org.neo4j.impl.util.ArrayMap;

/**
 * {@link XaConnection} implementation for the NioNeo data store. Contains
 * getter methods for the different stores (node,relationship,property and
 * relationship type).
 * <p>
 * A <CODE>NeoStoreXaConnection</CODE> is obtained from
 * {@link NeoStoreXaDataSource} and then Neo persistence layer can perform the
 * operations requested via the store implementations.
 */
public class NeoStoreXaConnection extends XaConnectionHelpImpl
{
    private final NeoStoreXaResource xaResource;

    private final NeoStore neoStore;
    private final NodeEventConsumer nodeConsumer;
    private final RelationshipEventConsumer relConsumer;
    private final RelationshipTypeEventConsumer relTypeConsumer;
    private final PropertyIndexEventConsumer propIndexConsumer;

    private NeoTransaction neoTransaction = null;

    NeoStoreXaConnection( NeoStore neoStore, XaResourceManager xaRm,
        byte branchId[] )
    {
        super( xaRm );
        this.neoStore = neoStore;

        this.nodeConsumer = new NodeEventConsumerImpl( this );
        this.relConsumer = new RelationshipEventConsumerImpl( this );
        this.relTypeConsumer = new RelationshipTypeEventConsumerImpl( this );
        this.propIndexConsumer = new PropertyIndexEventConsumerImpl( this );
        this.xaResource = new NeoStoreXaResource(
            neoStore.getStorageFileName(), xaRm, branchId );
    }

    /**
     * Returns this neo store's {@link NodeStore}.
     * 
     * @return The node store
     */
    public NodeEventConsumer getNodeConsumer()
    {
        return nodeConsumer;
    }

    /**
     * Returns this neo store's {@link RelationshipStore}.
     * 
     * @return The relationship store
     */
    public RelationshipEventConsumer getRelationshipConsumer()
    {
        return relConsumer;
    }

    public PropertyIndexEventConsumer getPropertyIndexConsumer()
    {
        return propIndexConsumer;
    }

    /**
     * Returns this neo store's {@link RelationshipTypeStore}.
     * 
     * @return The relationship type store
     */
    public RelationshipTypeEventConsumer getRelationshipTypeConsumer()
    {
        return relTypeConsumer;
    }

    /**
     * Made public for testing, dont use.
     */
    public PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    NodeStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    RelationshipTypeStore getRelationshipTypeStore()
    {
        return neoStore.getRelationshipTypeStore();
    }

    public XAResource getXaResource()
    {
        return this.xaResource;
    }

    NeoTransaction getNeoTransaction()
    {
        if ( neoTransaction != null )
        {
            return neoTransaction;
        }
        try
        {
            neoTransaction = (NeoTransaction) getTransaction();
            return neoTransaction;
        }
        catch ( XAException e )
        {
            throw new StoreFailureException( "Unable to get transaction.", e );
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

    private class NodeEventConsumerImpl implements NodeEventConsumer
    {
        private final NeoStoreXaConnection xaCon;
        private final NodeStore nodeStore;

        public NodeEventConsumerImpl( NeoStoreXaConnection xaCon )
        {
            this.xaCon = xaCon;
            nodeStore = getNodeStore();
        }

        public void createNode( int nodeId )
        {
            xaCon.getNeoTransaction().nodeCreate( nodeId );
        }

        public void deleteNode( int nodeId )
        {
            xaCon.getNeoTransaction().nodeDelete( nodeId );
        }

        // checks for created in tx else get from store
        public boolean loadLightNode( int nodeId )
        {
            return xaCon.getNeoTransaction().nodeLoadLight( nodeId );
        }

        public void addProperty( int nodeId, int propertyId,
            PropertyIndex index, Object value )
        {
            xaCon.getNeoTransaction().nodeAddProperty( nodeId, propertyId,
                index, value );
        }

        public void changeProperty( int nodeId, int propertyId, Object value )
        {
            xaCon.getNeoTransaction().nodeChangeProperty( nodeId, propertyId,
                value );
        }

        public void removeProperty( int nodeId, int propertyId )
        {
            xaCon.getNeoTransaction().nodeRemoveProperty( nodeId, propertyId );
        }

        public ArrayMap<Integer,PropertyData> getProperties( int nodeId )
        {
            return xaCon.getNeoTransaction().nodeGetProperties( nodeId );
        }

        public Iterable<RelationshipData> getRelationships( int nodeId )
        {
            return xaCon.getNeoTransaction().nodeGetRelationships( nodeId );
        }
    };

    private class RelationshipEventConsumerImpl implements
        RelationshipEventConsumer
    {
        private final NeoStoreXaConnection xaCon;
        private final RelationshipStore relStore;

        public RelationshipEventConsumerImpl( NeoStoreXaConnection xaCon )
        {
            this.xaCon = xaCon;
            this.relStore = getRelationshipStore();
        }

        public void createRelationship( int id, int firstNode, int secondNode,
            int type )
        {
            xaCon.getNeoTransaction().relationshipCreate( id, firstNode,
                secondNode, type );
        }

        public void deleteRelationship( int id )
        {
            xaCon.getNeoTransaction().relDelete( id );
        }

        public void addProperty( int relId, int propertyId,
            PropertyIndex index, Object value )
        {
            xaCon.getNeoTransaction().relAddProperty( relId, propertyId, index,
                value );
        }

        public void changeProperty( int relId, int propertyId, Object value )
        {
            xaCon.getNeoTransaction().relChangeProperty( relId, propertyId,
                value );
        }

        public void removeProperty( int relId, int propertyId )
        {
            xaCon.getNeoTransaction().relRemoveProperty( relId, propertyId );
        }

        public ArrayMap<Integer,PropertyData> getProperties( int relId )
        {
            return xaCon.getNeoTransaction().relGetProperties( relId );
        }

        public RelationshipData getRelationship( int id )
        {
            return xaCon.getNeoTransaction().relationshipLoad( id );
        }
    };

    private class RelationshipTypeEventConsumerImpl implements
        RelationshipTypeEventConsumer
    {
        private final NeoStoreXaConnection xaCon;
        private final RelationshipTypeStore relTypeStore;

        RelationshipTypeEventConsumerImpl( NeoStoreXaConnection xaCon )
        {
            this.xaCon = xaCon;
            this.relTypeStore = getRelationshipTypeStore();
        }

        public void addRelationshipType( int id, String name )
        {
            xaCon.getNeoTransaction().relationshipTypeAdd( id, name );
        }

        public RelationshipTypeData getRelationshipType( int id )
        {
            return relTypeStore.getRelationshipType( id );
        }

        public RelationshipTypeData[] getRelationshipTypes()
        {
            return relTypeStore.getRelationshipTypes();
        }
    };

    private static class PropertyIndexEventConsumerImpl implements
        PropertyIndexEventConsumer
    {
        private final NeoStoreXaConnection xaCon;

        PropertyIndexEventConsumerImpl( NeoStoreXaConnection xaCon )
        {
            this.xaCon = xaCon;
        }

        public void createPropertyIndex( int id, String key )
        {
            xaCon.getNeoTransaction().createPropertyIndex( id, key );
        }

        public String getKeyFor( int id )
        {
            return xaCon.getNeoTransaction().getPropertyIndex( id );
        }

        public PropertyIndexData[] getPropertyIndexes( int count )
        {
            return xaCon.getNeoTransaction().getPropertyIndexes( count );
        }
    };
}