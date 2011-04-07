/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexData;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipChainPosition;
import org.neo4j.kernel.impl.nioneo.store.RelationshipData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnectionHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;

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
    private final NodeEventConsumer nodeConsumer;
    private final RelationshipEventConsumer relConsumer;
    private final RelationshipTypeEventConsumer relTypeConsumer;
    private final PropertyIndexEventConsumer propIndexConsumer;

    private WriteTransaction neoTransaction = null;

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
     * Returns this the {@link NodeStore}.
     * 
     * @return The node store
     */
    public NodeEventConsumer getNodeConsumer()
    {
        return nodeConsumer;
    }

    /**
     * Returns this the {@link RelationshipStore}.
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
     * Returns this the {@link RelationshipTypeStore}.
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

    CommonAbstractStore getNodeStore()
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

    WriteTransaction getWriteTransaction()
    {
        if ( neoTransaction != null )
        {
            return neoTransaction;
        }
        try
        {
            neoTransaction = (WriteTransaction) getTransaction();
            return neoTransaction;
        }
        catch ( XAException e )
        {
            throw new TransactionFailureException( 
                "Unable to get transaction.", e );
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

    private static class NodeEventConsumerImpl implements NodeEventConsumer
    {
        private final NeoStoreXaConnection xaCon;

        public NodeEventConsumerImpl( NeoStoreXaConnection xaCon )
        {
            this.xaCon = xaCon;
        }

        public void createNode( long nodeId )
        {
            xaCon.getWriteTransaction().nodeCreate( nodeId );
        }

        public ArrayMap<Integer,PropertyData> deleteNode( long nodeId )
        {
            return xaCon.getWriteTransaction().nodeDelete( nodeId );
        }

        // checks for created in tx else get from store
        public boolean loadLightNode( long nodeId )
        {
            return xaCon.getWriteTransaction().nodeLoadLight( nodeId );
        }

        public void addProperty( long nodeId, long propertyId,
            PropertyIndex index, Object value )
        {
            xaCon.getWriteTransaction().nodeAddProperty( nodeId, propertyId,
                index, value );
        }

        public void changeProperty( long nodeId, long propertyId, Object value )
        {
            xaCon.getWriteTransaction().nodeChangeProperty( nodeId, propertyId,
                value );
        }

        public void removeProperty( long nodeId, long propertyId )
        {
            xaCon.getWriteTransaction().nodeRemoveProperty( nodeId, propertyId );
        }

        public ArrayMap<Integer,PropertyData> getProperties( long nodeId,
                boolean light )
        {
            return xaCon.getWriteTransaction().nodeGetProperties( nodeId, 
                    light );
        }

        public RelIdArray getCreatedNodes()
        {
            return xaCon.getWriteTransaction().getCreatedNodes();
        }

        public boolean isNodeCreated( long nodeId )
        {
            return xaCon.getWriteTransaction().nodeCreated( nodeId );
        }
    };

    private static class RelationshipEventConsumerImpl implements
        RelationshipEventConsumer
    {
        private final NeoStoreXaConnection xaCon;

        public RelationshipEventConsumerImpl( NeoStoreXaConnection xaCon )
        {
            this.xaCon = xaCon;
        }

        public void createRelationship( long id, long firstNode, long secondNode,
            int type )
        {
            xaCon.getWriteTransaction().relationshipCreate( id, firstNode,
                secondNode, type );
        }

        public ArrayMap<Integer,PropertyData> deleteRelationship( long id )
        {
            return xaCon.getWriteTransaction().relDelete( id );
        }

        public void addProperty( long relId, long propertyId,
            PropertyIndex index, Object value )
        {
            xaCon.getWriteTransaction().relAddProperty( relId, propertyId, index,
                value );
        }

        public void changeProperty( long relId, long propertyId, Object value )
        {
            xaCon.getWriteTransaction().relChangeProperty( relId, propertyId,
                value );
        }

        public void removeProperty( long relId, long propertyId )
        {
            xaCon.getWriteTransaction().relRemoveProperty( relId, propertyId );
        }

        public ArrayMap<Integer,PropertyData> getProperties( long relId,
                boolean light )
        {
            return xaCon.getWriteTransaction().relGetProperties( relId, light );
        }

        public RelationshipData getRelationship( long id )
        {
            return xaCon.getWriteTransaction().relationshipLoad( id );
        }

        public RelationshipChainPosition getRelationshipChainPosition( 
            long nodeId )
        {
            return xaCon.getWriteTransaction().getRelationshipChainPosition( 
                nodeId );
        }

        public Iterable<RelationshipData> getMoreRelationships( long nodeId,
            RelationshipChainPosition position )
        {
            return xaCon.getWriteTransaction().getMoreRelationships( nodeId, 
                position );
        }

        public boolean isRelationshipCreated( long relId )
        {
            return xaCon.getWriteTransaction().relCreated( relId );
        }
    };

    private static class RelationshipTypeEventConsumerImpl implements
        RelationshipTypeEventConsumer
    {
        private final NeoStoreXaConnection xaCon;
        private final RelationshipTypeStore relTypeStore;

        RelationshipTypeEventConsumerImpl( NeoStoreXaConnection xaCon )
        {
            this.xaCon = xaCon;
            this.relTypeStore = xaCon.getRelationshipTypeStore();
        }

        public void addRelationshipType( int id, String name )
        {
            xaCon.getWriteTransaction().relationshipTypeAdd( id, name );
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
            xaCon.getWriteTransaction().createPropertyIndex( id, key );
        }

        public String getKeyFor( int id )
        {
            return xaCon.getWriteTransaction().getPropertyIndex( id );
        }

        public PropertyIndexData[] getPropertyIndexes( int count )
        {
            return xaCon.getWriteTransaction().getPropertyIndexes( count );
        }
    };
}