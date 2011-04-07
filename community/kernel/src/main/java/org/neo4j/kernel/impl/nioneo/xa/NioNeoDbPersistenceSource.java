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

import javax.transaction.xa.XAResource;

import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexData;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipChainPosition;
import org.neo4j.kernel.impl.nioneo.store.RelationshipData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.persistence.ResourceConnection;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;

/**
 * The NioNeo persistence source implementation. If this class is registered as
 * persistence source for Neo4j kernel operations that are performed on the node space
 * will be forwarded to this class {@link ResourceConnection} implementation.
 */
public class NioNeoDbPersistenceSource implements PersistenceSource
{
    private static final String MODULE_NAME = "NioNeoDbPersistenceSource";

    private NeoStoreXaDataSource xaDs = null;
    private String dataSourceName = null;
    private ResourceConnection readOnlyResourceConnection; 

    public synchronized void init()
    {
        // Do nothing
    }

    public synchronized void start( XaDataSourceManager xaDsManager )
    {
        xaDs = (NeoStoreXaDataSource) xaDsManager.getXaDataSource( "nioneodb" );
        if ( xaDs == null )
        {
            throw new IllegalStateException( 
                "Unable to get nioneodb datasource" );
        }
        readOnlyResourceConnection = new ReadOnlyResourceConnection( xaDs );
    }

    public synchronized void reload()
    {
        // Do nothing
    }

    public synchronized void stop()
    {
        if ( xaDs != null )
        {
            xaDs.close();
        }
    }

    public synchronized void destroy()
    {
        // Do nothing
    }

    public String getModuleName()
    {
        return MODULE_NAME;
    }

    public ResourceConnection createResourceConnection()
    {
        if ( xaDs.isReadOnly() )
        {
            throw new ReadOnlyDbException();
        }
        return new NioNeoDbResourceConnection( this.xaDs );
    }
    
    public ResourceConnection createReadOnlyResourceConnection()
    {
        return readOnlyResourceConnection; 
    }
    
    private static class ReadOnlyResourceConnection implements 
        ResourceConnection
    {
        private final ReadTransaction readTransaction;
        private final RelationshipTypeStore relTypeStore;

        ReadOnlyResourceConnection( NeoStoreXaDataSource xaDs )
        {
            this.readTransaction = xaDs.getReadOnlyTransaction();
            this.relTypeStore = xaDs.getNeoStore().getRelationshipTypeStore();
        }

        public XAResource getXAResource()
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }
        
        public void destroy()
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public ArrayMap<Integer,PropertyData> nodeDelete( long nodeId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public long nodeAddProperty( long nodeId, PropertyIndex index,
            Object value )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void nodeChangeProperty( long nodeId, long propertyId, Object value )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void nodeRemoveProperty( long nodeId, long propertyId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void nodeCreate( long nodeId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void relationshipCreate( long id, int typeId, long startNodeId,
            long endNodeId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public ArrayMap<Integer,PropertyData> relDelete( long relId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public long relAddProperty( long relId, PropertyIndex index, Object value )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void relChangeProperty( long relId, long propertyId, Object value )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void relRemoveProperty( long relId, long propertyId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public String loadIndex( int id )
        {
            return readTransaction.getPropertyIndex( id );
        }

        public PropertyIndexData[] loadPropertyIndexes( int maxCount )
        {
            return readTransaction.getPropertyIndexes( maxCount );
        }

        public Object loadPropertyValue( long id )
        {
            return readTransaction.propertyGetValue( id );
        }

        public RelationshipTypeData[] loadRelationshipTypes()
        {
            RelationshipTypeData relTypeData[] = 
                relTypeStore.getRelationshipTypes();
            RelationshipTypeData rawRelTypeData[] = 
                new RelationshipTypeData[relTypeData.length];
            for ( int i = 0; i < relTypeData.length; i++ )
            {
                rawRelTypeData[i] = new RelationshipTypeData( 
                    relTypeData[i].getId(), relTypeData[i].getName() );
            }
            return rawRelTypeData;
        }

        public boolean nodeLoadLight( long id )
        {
            return readTransaction.nodeLoadLight( id );
        }

        public ArrayMap<Integer,PropertyData> nodeLoadProperties( long nodeId,
                boolean light )
        {
            // ignore light load
            return readTransaction.nodeGetProperties( nodeId );
        }

        public RelationshipData relLoadLight( long id )
        {
            return readTransaction.relationshipLoad( id );
        }

        public ArrayMap<Integer,PropertyData> relLoadProperties( long relId,
                boolean light )
        {
            // ignore light load
            return readTransaction.relGetProperties( relId );
        }

        public void createPropertyIndex( String key, int id )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void createRelationshipType( int id, String name )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public RelationshipChainPosition getRelationshipChainPosition( 
            long nodeId )
        {
            return readTransaction.getRelationshipChainPosition( nodeId );
        }

        public Iterable<RelationshipData> getMoreRelationships( long nodeId,
            RelationshipChainPosition position )
        {
            return readTransaction.getMoreRelationships( nodeId, position );
        }

        public RelIdArray getCreatedNodes()
        {
            return new RelIdArray();
        }

        public boolean isNodeCreated( long nodeId )
        {
            return false;
        }

        public boolean isRelationshipCreated( long relId )
        {
            return false;
        }

        public int getKeyIdForProperty( long propertyId )
        {
            return readTransaction.getKeyIdForProperty( propertyId );
        }
    }

    private static class NioNeoDbResourceConnection implements
        ResourceConnection
    {
        private NeoStoreXaConnection xaCon;
        private NodeEventConsumer nodeConsumer;
        private RelationshipEventConsumer relConsumer;
        private RelationshipTypeEventConsumer relTypeConsumer;
        private PropertyIndexEventConsumer propIndexConsumer;
        private PropertyStore propStore;

        NioNeoDbResourceConnection( NeoStoreXaDataSource xaDs )
        {
            this.xaCon = (NeoStoreXaConnection) xaDs.getXaConnection();
            nodeConsumer = xaCon.getNodeConsumer();
            relConsumer = xaCon.getRelationshipConsumer();
            relTypeConsumer = xaCon.getRelationshipTypeConsumer();
            propIndexConsumer = xaCon.getPropertyIndexConsumer();
            propStore = xaCon.getPropertyStore();
        }

        public XAResource getXAResource()
        {
            return this.xaCon.getXaResource();
        }
        
        public void destroy()
        {
            xaCon.destroy();
            xaCon = null;
            nodeConsumer = null;
            relConsumer = null;
            relTypeConsumer = null;
            propIndexConsumer = null;
        }

        public ArrayMap<Integer,PropertyData> nodeDelete( long nodeId )
        {
            return nodeConsumer.deleteNode( nodeId );
        }

        public long nodeAddProperty( long nodeId, PropertyIndex index,
            Object value )
        {
            long propertyId = propStore.nextId();
            nodeConsumer.addProperty( nodeId, propertyId, index, value );
            return propertyId;
        }

        public void nodeChangeProperty( long nodeId, long propertyId, Object value )
        {
            nodeConsumer.changeProperty( nodeId, propertyId, value );
        }

        public void nodeRemoveProperty( long nodeId, long propertyId )
        {
            nodeConsumer.removeProperty( nodeId, propertyId );
        }

        public void nodeCreate( long nodeId )
        {
            nodeConsumer.createNode( nodeId );
        }

        public void relationshipCreate( long id, int typeId, long startNodeId,
            long endNodeId )
        {
            relConsumer.createRelationship( id, startNodeId, endNodeId, typeId );
        }

        public ArrayMap<Integer,PropertyData> relDelete( long relId )
        {
            return relConsumer.deleteRelationship( relId );
        }

        public long relAddProperty( long relId, PropertyIndex index, Object value )
        {
            long propertyId = propStore.nextId();
            relConsumer.addProperty( relId, propertyId, index, value );
            return propertyId;
        }

        public void relChangeProperty( long relId, long propertyId, Object value )
        {
            relConsumer.changeProperty( relId, propertyId, value );
        }

        public void relRemoveProperty( long relId, long propertyId )
        {
            relConsumer.removeProperty( relId, propertyId );
        }

        public String loadIndex( int id )
        {
            return propIndexConsumer.getKeyFor( id );
        }

        public PropertyIndexData[] loadPropertyIndexes( int maxCount )
        {
            return propIndexConsumer.getPropertyIndexes( maxCount );
        }

        public Object loadPropertyValue( long id )
        {
            return xaCon.getWriteTransaction().propertyGetValue( id );
        }

        public RelationshipTypeData[] loadRelationshipTypes()
        {
            RelationshipTypeData relTypeData[] = 
                relTypeConsumer.getRelationshipTypes();
            RelationshipTypeData rawRelTypeData[] = 
                new RelationshipTypeData[relTypeData.length];
            for ( int i = 0; i < relTypeData.length; i++ )
            {
                rawRelTypeData[i] = new RelationshipTypeData( 
                    relTypeData[i].getId(), relTypeData[i].getName() );
            }
            return rawRelTypeData;
        }

        public boolean nodeLoadLight( long id )
        {
            return nodeConsumer.loadLightNode( id );
        }

        public ArrayMap<Integer,PropertyData> nodeLoadProperties( long nodeId,
                boolean light )
        {
            return nodeConsumer.getProperties( nodeId, light );
        }

        public RelationshipData relLoadLight( long id )
        {
            return relConsumer.getRelationship( id );
        }

        public ArrayMap<Integer,PropertyData> relLoadProperties( long relId,
                boolean light )
        {
            return relConsumer.getProperties( relId, light );
        }

        public void createPropertyIndex( String key, int id )
        {
            propIndexConsumer.createPropertyIndex( id, key );
        }

        public void createRelationshipType( int id, String name )
        {
            relTypeConsumer.addRelationshipType( id, name );
        }

        public RelationshipChainPosition getRelationshipChainPosition( 
            long nodeId )
        {
            return relConsumer.getRelationshipChainPosition( nodeId );
        }

        public Iterable<RelationshipData> getMoreRelationships( long nodeId,
            RelationshipChainPosition position )
        {
            return relConsumer.getMoreRelationships( nodeId, position );
        }

        public RelIdArray getCreatedNodes()
        {
            return nodeConsumer.getCreatedNodes();
        }

        public boolean isNodeCreated( long nodeId )
        {
            return nodeConsumer.isNodeCreated( nodeId );
        }

        public boolean isRelationshipCreated( long relId )
        {
            return relConsumer.isRelationshipCreated( relId );
        }

        public int getKeyIdForProperty( long propertyId )
        {
            return xaCon.getWriteTransaction().getKeyIdForProperty( propertyId );
        }
    }

    public String toString()
    {
        return "A persistence source to [" + dataSourceName + "]";
    }

    public long nextId( Class<?> clazz )
    {
        return xaDs.nextId( clazz );
    }

    // for recovery, returns a xa
    public XAResource getXaResource()
    {
        return this.xaDs.getXaConnection().getXaResource();
    }

    public void setDataSourceName( String dataSourceName )
    {
        this.dataSourceName = dataSourceName;
    }

    public String getDataSourceName()
    {
        return this.dataSourceName;
    }

    public long getHighestPossibleIdInUse( Class<?> clazz )
    {
        return xaDs.getHighestPossibleIdInUse( clazz );
    }

    public long getNumberOfIdsInUse( Class<?> clazz )
    {
        return xaDs.getNumberOfIdsInUse( clazz );
    }
    
    public XaDataSource getXaDataSource()
    {
        return xaDs;
    }
}