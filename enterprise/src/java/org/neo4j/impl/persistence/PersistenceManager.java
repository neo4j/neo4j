/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.persistence;

import javax.transaction.TransactionManager;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.core.RawNodeData;
import org.neo4j.impl.core.RawPropertyData;
import org.neo4j.impl.core.RawPropertyIndex;
import org.neo4j.impl.core.RawRelationshipData;
import org.neo4j.impl.core.RawRelationshipTypeData;

/**
 * The PersistenceManager is the front-end for all persistence related
 * operations. In reality, only <B>load</B> operations are accessible via the
 * PersistenceManager due to Neo's incremental persistence architecture
 * -- updates, additions and deletions are handled via the event framework and
 * the {@link PersistenceLayerMonitor}.
 */
public class PersistenceManager
{
	private final ResourceBroker broker;
	
	public PersistenceManager( TransactionManager transactionManager ) 
	{ 
		broker = new ResourceBroker( transactionManager );
	}
	
	ResourceBroker getResourceBroker()
	{
		return broker;
	}

	public RawNodeData loadLightNode( int id )
	{
        return getResource().nodeLoadLight( id );
	}
	
	public Object loadPropertyValue( int id )
	{
        return getResource().loadPropertyValue( id );
	}
	
	public String loadIndex( int id )
	{
        return getResource().loadIndex( id );
	}
	
	public RawPropertyIndex[] loadPropertyIndexes( int maxCount )
	{
        return getResource().loadPropertyIndexes( maxCount );
	}
	
	public RawRelationshipData[] loadRelationships( int nodeId )
	{
        return getResource().nodeLoadRelationships( nodeId );
	}

	public RawPropertyData[] loadNodeProperties( int nodeId )
	{
        return getResource().nodeLoadProperties( nodeId );
	}
	
	public RawPropertyData[] loadRelProperties( int relId )
	{
        return getResource().relLoadProperties( relId );
	}

	public RawRelationshipData loadLightRelationship( int id )
	{
        return getResource().relLoadLight( id );
	}
	
	public RawRelationshipTypeData[] loadAllRelationshipTypes()
	{
        return getResource().loadRelationshipTypes();
	}
	
	private ResourceConnection getResource()
	{
		return broker.acquireResourceConnection();
	}

    public void nodeDelete( int nodeId )
    {
        getResource().nodeDelete( nodeId );
    }

    public int nodeAddProperty( int nodeId, PropertyIndex index, Object value )
    {
        return getResource().nodeAddProperty( nodeId, index, value );
    }

    public void nodeChangeProperty( int nodeId, int propertyId, Object value )
    {
        getResource().nodeChangeProperty( nodeId, propertyId, value );
    }

    public void nodeRemoveProperty( int nodeId, int propertyId )
    {
        getResource().nodeRemoveProperty( nodeId, propertyId );
    }

    public void nodeCreate( int id )
    {
        getResource().nodeCreate( id );
    }

    public void relationshipCreate( int id, int typeId, int startNodeId, 
        int endNodeId )
    {
        getResource().relationshipCreate( id, typeId, startNodeId, endNodeId );
    }	

    public void relDelete( int relId )
    {
        getResource().relDelete( relId );
    }

    public int relAddProperty( int relId, PropertyIndex index, Object value )
    {
        return getResource().relAddProperty( relId, index, value );
    }

    public void relChangeProperty( int relId, int propertyId, Object value )
    {
        getResource().relChangeProperty( relId, propertyId, value );
    }

    public void relRemoveProperty( int relId, int propertyId )
    {
        getResource().relRemoveProperty( relId, propertyId );
    }

    public void createPropertyIndex( String key, int id )
    {
        getResource().createPropertyIndex( key, id );
    }

    public void createRelationshipType( int id, String name )
    {
        getResource().createRelationshipType( id, name );
    }
}