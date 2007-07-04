package org.neo4j.impl.persistence;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.core.NotFoundException;
import org.neo4j.impl.core.RawNodeData;
import org.neo4j.impl.core.RawPropertyData;
import org.neo4j.impl.core.RawRelationshipData;
import org.neo4j.impl.core.RawRelationshipTypeData;
import org.neo4j.impl.transaction.NotInTransactionException;


// JTA imports
import javax.transaction.xa.XAResource;

/**
 * The PersistenceManager is the front-end for all persistence related
 * operations. In reality, only <B>load</B> operations are accessible via the
 * PersistenceManager due to Neo's incremental persistence architecture
 * -- updates, additions and deletions are handled via the event framework and
 * the {@link BusinessLayerMonitor}.
 */
public class PersistenceManager
{
	// -- Constants
	
	// Four constants representing persistence operations. They are of the
	// type PersistenceManager.Operation, see inner class definition at the
	// end of this file.

	/** A constant representing a persistence operation that loads a node
	 * into {@link #loadLightNode phase I}. */
	public static final Operation LOAD_LIGHT_NODE = 
											new Operation( "LOAD_LIGHT_NODE");

	public static final Operation LOAD_NODE_PROPERTIES = 
		new Operation( "LOAD_NODE_PROPERTIES" );
		
	public static final Operation LOAD_REL_PROPERTIES = 
		new Operation( "LOAD_REL_PROPERTIES" );
		
	public static final Operation LOAD_RELATIONSHIPS = 
		new Operation( "LOAD_RELATIONSHIPS" );
		
	public static final Operation LOAD_PROPERTY_VALUE = 
		new Operation( "LOAD_PROPERTY_VALUE" );
	
	/** A constant representing a persistence operation that loads a
	 * {@link #loadLightRelationship shallow relationship}. */
	public static final Operation LOAD_LIGHT_REL =  
											new Operation( "LOAD_LIGHT_REL" );
		
	/** A constant representing a persistence operation that loads all 
	 * {@link #loadAllRelationshipTypes relationship types} from
	 * persistent storage. */
	public static final Operation LOAD_ALL_RELATIONSHIP_TYPES = 
								new Operation( "LOAD_ALL_RELATIONSHIP_TYPES" );
	
	// -- Singleton operations
	
	private static PersistenceManager instance = new PersistenceManager();
	private PersistenceManager() { }
	/**
	 * Singleton accessor.
	 * @return the singleton persistence manager
	 */
	public static PersistenceManager getManager() {	return instance; }
	
	
	// -- Attributes
	// This is a dummy implementation, obviously. We need it to acquire
	// a PersistenceSource from the PersistenceSourceDispatcher, but
	// since the PSD is currently not production implemented it doesn't
	// even use the PersistenceMetadata that it gets. So, therefore this
	// dumb implementation.
	private final PersistenceMetadata dummyMeta = new PersistenceMetadata()
												{
													public Object getEntity()
													{
														return this;
													}
												};
	
	// -- Persistence operations
	
	public RawNodeData loadLightNode( int id ) throws PersistenceException
	{
		return (RawNodeData) getResource().performOperation( LOAD_LIGHT_NODE, 
			id );
	}
	
	public Object loadPropertyValue( int id )
		throws PersistenceException
	{
		return getResource().performOperation( LOAD_PROPERTY_VALUE, id );
	}
	
	public RawRelationshipData[] loadRelationships( Node node )
		throws PersistenceException
	{
		return ( RawRelationshipData[] ) getResource().performOperation( 
			LOAD_RELATIONSHIPS, node );
	}

	public RawPropertyData[] loadProperties( Node node )
		throws PersistenceException
	{
		return ( RawPropertyData[] ) getResource().performOperation( 
			LOAD_NODE_PROPERTIES, node );
	}
	
	public RawPropertyData[] loadProperties( Relationship relationship )
		throws PersistenceException
	{
		return ( RawPropertyData[] ) getResource().performOperation( 
			LOAD_REL_PROPERTIES, relationship );
	}

	public RawRelationshipData loadLightRelationship( int id )
		throws NotFoundException, PersistenceException
	{
		return (RawRelationshipData) getResource().performOperation( 
			LOAD_LIGHT_REL, id );
	}
	
	public RawRelationshipTypeData[] loadAllRelationshipTypes()
		throws PersistenceException
	{
		return (RawRelationshipTypeData[]) getResource().performOperation( 
			LOAD_ALL_RELATIONSHIP_TYPES , null );
	}

	// -- Utility operations
	
	private ResourceConnection getResource()
		throws PersistenceException
	{
		try
		{
			ResourceConnection res			= null;
			ResourceBroker broker			= ResourceBroker.getBroker();
			
			res = broker.acquireResourceConnection( dummyMeta );
			return res;
		}
		catch ( NotInTransactionException nite )
		{
			throw new PersistenceException( nite ); // this is enough info
		}
		catch ( ResourceAcquisitionFailedException rafe )
		{
			throw new PersistenceException( "Unable to acquire resource " +
											"connection to persistence source",
											rafe );
		}
	}
	
	
	// used by TM for recovering
	public XAResource getXaResource( byte branchId[] )
	{
		return ResourceBroker.getBroker().getXaResource( branchId );
	}
	
	// -- Inner classes
	/**
	 * A package private inner typesafe enum that represents a persistence
	 * operation. It is used to denote a particular persistence operation,
	 * for example:
	 * <CODE>
	 * <PRE>
	 *      // in SomeClass:
	 *      public void execute( PersistenceManager.Operation op, Object obj )
	 *      {
	 *              // ...
	 *      }
	 *
	 *      // in client of SomeClass:
	 *      SomeClass someObj = ...
	 *      someObj.execute( PersistenceManager.OPERATION_LOAD_FULL_NODE, ... );
	 * </PRE>
	 * </CODE>
	 */
	public static final class Operation
	{
		private String name = null;
		Operation( String name ) { this.name = name; }
		public String toString() { return this.name; }
	} // end inner class operation
	
} // end persistence manager
