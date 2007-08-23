package org.neo4j.impl.persistence;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.core.NotFoundException;
import org.neo4j.impl.core.RawNodeData;
import org.neo4j.impl.core.RawPropertyData;
import org.neo4j.impl.core.RawRelationshipData;
import org.neo4j.impl.core.RawRelationshipTypeData;
import org.neo4j.impl.transaction.NotInTransactionException;

/**
 * The PersistenceManager is the front-end for all persistence related
 * operations. In reality, only <B>load</B> operations are accessible via the
 * PersistenceManager due to Neo's incremental persistence architecture
 * -- updates, additions and deletions are handled via the event framework and
 * the {@link BusinessLayerMonitor}.
 */
public class PersistenceManager
{
	private static final PersistenceManager instance = new PersistenceManager();
	private PersistenceManager() { }

	public static PersistenceManager getManager() {	return instance; }
	
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
			ResourceBroker broker = ResourceBroker.getBroker();
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