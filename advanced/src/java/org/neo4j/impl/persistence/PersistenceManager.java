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
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.core.NotFoundException;
import org.neo4j.impl.core.RawNodeData;
import org.neo4j.impl.core.RawPropertyData;
import org.neo4j.impl.core.RawPropertyIndex;
import org.neo4j.impl.core.RawRelationshipData;
import org.neo4j.impl.core.RawRelationshipTypeData;
import org.neo4j.impl.transaction.NotInTransactionException;

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
	
	// private static final PersistenceManager instance = new PersistenceManager();
	public PersistenceManager( TransactionManager transactionManager ) 
	{ 
		broker = new ResourceBroker( transactionManager );
	}
	
	ResourceBroker getResourceBroker()
	{
		return broker;
	}

	// public static PersistenceManager getManager() {	return instance; }
	
	public RawNodeData loadLightNode( int id ) throws PersistenceException
	{
		return (RawNodeData) getResource().performOperation( 
			Operation.LOAD_LIGHT_NODE, id );
	}
	
	public Object loadPropertyValue( int id )
		throws PersistenceException
	{
		return getResource().performOperation( Operation.LOAD_PROPERTY_VALUE, 
			id );
	}
	
	public String loadIndex( int id )
		throws PersistenceException
	{
		return (String) getResource().performOperation( 
			Operation.LOAD_PROPERTY_INDEX, id );
	}
	
	public RawPropertyIndex[] loadPropertyIndexes( int maxCount )
		throws PersistenceException
	{
		return (RawPropertyIndex[]) getResource().performOperation( 
			Operation.LOAD_PROPERTY_INDEXES, maxCount );
	}
	
	public RawRelationshipData[] loadRelationships( Node node )
		throws PersistenceException
	{
		return ( RawRelationshipData[] ) getResource().performOperation( 
			Operation.LOAD_RELATIONSHIPS, node );
	}

	public RawPropertyData[] loadProperties( Node node )
		throws PersistenceException
	{
		return ( RawPropertyData[] ) getResource().performOperation( 
			Operation.LOAD_NODE_PROPERTIES, node );
	}
	
	public RawPropertyData[] loadProperties( Relationship relationship )
		throws PersistenceException
	{
		return ( RawPropertyData[] ) getResource().performOperation( 
			Operation.LOAD_REL_PROPERTIES, relationship );
	}

	public RawRelationshipData loadLightRelationship( int id )
		throws NotFoundException, PersistenceException
	{
		return (RawRelationshipData) getResource().performOperation( 
			Operation.LOAD_LIGHT_REL, id );
	}
	
	public RawRelationshipTypeData[] loadAllRelationshipTypes()
		throws PersistenceException
	{
		return (RawRelationshipTypeData[]) getResource().performOperation( 
			Operation.LOAD_ALL_RELATIONSHIP_TYPES , null );
	}
	
	private ResourceConnection getResource()
		throws PersistenceException
	{
		try
		{
			return broker.acquireResourceConnection(); // dummyMeta );
		}
		catch ( NotInTransactionException nite )
		{
			throw new PersistenceException( nite ); // this is enough info
		}
		catch ( ResourceAcquisitionFailedException rafe )
		{
			throw new PersistenceException( "Unable to acquire resource " +
				"connection to persistence source", rafe );
		}
	}	
}