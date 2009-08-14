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

import javax.transaction.xa.XAResource;

import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.impl.persistence.PersistenceSource;
import org.neo4j.impl.persistence.ResourceConnection;
import org.neo4j.impl.transaction.XaDataSourceManager;
import org.neo4j.impl.transaction.xaframework.XaDataSource;
import org.neo4j.impl.util.ArrayMap;

/**
 * The NioNeo persistence source implementation. If this class is registered as
 * persistence source for Neo operations that are performed on the node space
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
            throw new RuntimeException( "Unable to get nioneodb datasource" );
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
        return new NioNeoDbResourceConnection( this.xaDs );
    }
    
    public ResourceConnection createReadOnlyResourceConnection()
    {
        return readOnlyResourceConnection; 
    }
    
    private static class ReadOnlyResourceConnection implements 
        ResourceConnection
    {
        private final NeoReadTransaction neoTransaction;
        private final RelationshipTypeStore relTypeStore;

        ReadOnlyResourceConnection( NeoStoreXaDataSource xaDs )
        {
            this.neoTransaction = xaDs.getReadOnlyTransaction();
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

        public void nodeDelete( int nodeId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public int nodeAddProperty( int nodeId, PropertyIndex index,
            Object value )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void nodeChangeProperty( int nodeId, int propertyId, Object value )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void nodeRemoveProperty( int nodeId, int propertyId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void nodeCreate( int nodeId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void relationshipCreate( int id, int typeId, int startNodeId,
            int endNodeId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void relDelete( int relId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public int relAddProperty( int relId, PropertyIndex index, Object value )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void relChangeProperty( int relId, int propertyId, Object value )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public void relRemoveProperty( int relId, int propertyId )
        {
            throw new IllegalStateException( 
                "This is a read only transaction, " + 
                "this method should never be invoked" );
        }

        public String loadIndex( int id )
        {
            return neoTransaction.getPropertyIndex( id );
        }

        public PropertyIndexData[] loadPropertyIndexes( int maxCount )
        {
            return neoTransaction.getPropertyIndexes( maxCount );
        }

        public Object loadPropertyValue( int id )
        {
            return neoTransaction.propertyGetValue( id );
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

        public boolean nodeLoadLight( int id )
        {
            return neoTransaction.nodeLoadLight( id );
        }

        public ArrayMap<Integer,PropertyData> nodeLoadProperties( int nodeId )
        {
            return neoTransaction.nodeGetProperties( nodeId );
        }

        public Iterable<RelationshipData> nodeLoadRelationships( int nodeId )
        {
            return neoTransaction.nodeGetRelationships( nodeId );
        }

        public RelationshipData relLoadLight( int id )
        {
            return neoTransaction.relationshipLoad( id );
        }

        public ArrayMap<Integer,PropertyData> relLoadProperties( int relId )
        {
            return neoTransaction.relGetProperties( relId );
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

        public void nodeDelete( int nodeId )
        {
            nodeConsumer.deleteNode( nodeId );
        }

        public int nodeAddProperty( int nodeId, PropertyIndex index,
            Object value )
        {
            int propertyId = propStore.nextId();
            nodeConsumer.addProperty( nodeId, propertyId, index, value );
            return propertyId;
        }

        public void nodeChangeProperty( int nodeId, int propertyId, Object value )
        {
            nodeConsumer.changeProperty( nodeId, propertyId, value );
        }

        public void nodeRemoveProperty( int nodeId, int propertyId )
        {
            nodeConsumer.removeProperty( nodeId, propertyId );
        }

        public void nodeCreate( int nodeId )
        {
            nodeConsumer.createNode( nodeId );
        }

        public void relationshipCreate( int id, int typeId, int startNodeId,
            int endNodeId )
        {
            relConsumer.createRelationship( id, startNodeId, endNodeId, typeId );
        }

        public void relDelete( int relId )
        {
            relConsumer.deleteRelationship( relId );
        }

        public int relAddProperty( int relId, PropertyIndex index, Object value )
        {
            int propertyId = propStore.nextId();
            relConsumer.addProperty( relId, propertyId, index, value );
            return propertyId;
        }

        public void relChangeProperty( int relId, int propertyId, Object value )
        {
            relConsumer.changeProperty( relId, propertyId, value );
        }

        public void relRemoveProperty( int relId, int propertyId )
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

        public Object loadPropertyValue( int id )
        {
            return xaCon.getNeoTransaction().propertyGetValue( id );
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

        public boolean nodeLoadLight( int id )
        {
            return nodeConsumer.loadLightNode( id );
        }

        public ArrayMap<Integer,PropertyData> nodeLoadProperties( int nodeId )
        {
            return nodeConsumer.getProperties( nodeId );
        }

        public Iterable<RelationshipData> nodeLoadRelationships( int nodeId )
        {
            return nodeConsumer.getRelationships( nodeId );
        }

        public RelationshipData relLoadLight( int id )
        {
            return relConsumer.getRelationship( id );
        }

        public ArrayMap<Integer,PropertyData> relLoadProperties( int relId )
        {
            return relConsumer.getProperties( relId );
        }

        public void createPropertyIndex( String key, int id )
        {
            propIndexConsumer.createPropertyIndex( id, key );
        }

        public void createRelationshipType( int id, String name )
        {
            relTypeConsumer.addRelationshipType( id, name );
        }
    }

    public String toString()
    {
        return "A Nio Neo Db persistence source to [" + dataSourceName + "]";
    }

    public int nextId( Class<?> clazz )
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