package org.neo4j.impl.persistence;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.neo4j.impl.transaction.NotInTransactionException;
import org.neo4j.impl.transaction.TransactionFactory;

/**
 * The ResourceBroker is the access point for {@link ResourceConnection}s to
 * the persistence sources used. Whenever a component in the
 * persistence layer requires a persistence connection, it requests it from
 * the ResourceBroker via the {@link #acquireResourceConnection} method.
 * <P>
 * The main job of the ResourceBroker is to try to ensure that two
 * subsequent requests within the same transaction get the same connection.
 * This not only speeds up performance on our end (by enlisting/delisting
 * fewer resources in the transaction) but also saves resources in the
 * persistence backends.
 * <P>
 * Limitations:
 * <UL
 *	<LI>Access to {@link #acquireResourceConnection} is currently serialized.
 *		This may lead to monitor contention and lower performance.
 * </UL>
 */
class ResourceBroker
{
	private static Logger log = Logger.getLogger( 
		ResourceBroker.class.getName() );

	private Map<Transaction,ConnectionBundle> txConnectionMap = 
		new HashMap<Transaction,ConnectionBundle>();

	// A hook that releases resources after tx.commit
	private Synchronization txCommitHook = new TxCommitHook();
	
	private static ResourceBroker instance	= new ResourceBroker();
	private ResourceBroker() { }
	
	/**
	 * Singleton accessor.
	 * @return the singleton resource broker
	 */
	static ResourceBroker getBroker()
	{
		return instance;
	}
	
	/**
	 * Acquires the resource connection that should be used to persist
	 * the object represented by <CODE>meta</CODE>. This method looks up the
	 * invoking thread's transaction and if the resource connection is new to
	 * the transaction, the resource is enlisted according to JTA. Subsequent
	 * invokations of this method with the same parameter, will return the same 
	 * {@link ResourceConnection}.
	 * <P>
	 * This method is guaranteed to never return <CODE>null</CODE>.
	 * @param meta the metadata wrapper for the object that will be persisted
	 * with the returned resource connection 
	 * @return the {@link ResourceConnection} that should be used to persist
	 * the entity that is wrapped <CODE>meta</CODE>.
	 * @throws NotInTransactionException if the resource broker is unable to
	 * fetch a transaction for the current thread
	 * @throws ResourceAcquisitionFailedException if the resource broker is
	 * unable to acquire a resource connection for any reason other than
	 * <CODE>NotInTransaction</CODE>
	 */
	synchronized ResourceConnection acquireResourceConnection(
													PersistenceMetadata meta )
		throws	ResourceAcquisitionFailedException,
				NotInTransactionException
	{
		if ( meta == null || meta.getEntity() == null )
		{
			Object entity = null;
			if ( meta != null )
			{
				entity = meta.getEntity();
			}
			throw new IllegalArgumentException( "meta =" + meta + ", " +
												"meta.getEntity = " +
												entity );
		}
		
		ResourceConnection con		= null;
		ConnectionBundle bundle		= null;
		PersistenceSource source	= null;
		
		// Get persistence source for entity
		source = PersistenceSourceDispatcher.getDispatcher().
					getPersistenceSource( meta.getEntity() );
		
		// Get transaction for current thread
		Transaction tx			= this.getCurrentTransaction();

		// Get the bundle for this tx or create new one if one does not exist
		if ( txConnectionMap.containsKey(tx) )
		{
			bundle = txConnectionMap.get( tx );
		}
		else
		{
			try
			{
				bundle = new ConnectionBundle();
				tx.registerSynchronization( txCommitHook );
				txConnectionMap.put( tx, bundle );
			}
			catch ( Exception e )
			{
				throw new ResourceAcquisitionFailedException( 
					"Unable to register commit hook with current tx", e );
			}
		}
		
		// If we already have a connection for this guy, reuse it... 
		if ( bundle.hasConnectionForPersistenceSource(source) )
		{
			con = bundle.getConnectionForPersistenceSource( source );
		}
		else // ... or else, create a new connection and enlist it in tx
		{
			try
			{
				con = source.createResourceConnection();
				if ( !tx.enlistResource( con.getXAResource() ) )
				{
					throw new ResourceAcquisitionFailedException(
						"Unable to enlist '" + con.getXAResource() + "' in " +
						"transaction" );
				}
				bundle.setConnectionForPersistenceSource( source, con );
			}
			catch ( ConnectionCreationFailedException ccfe )
			{
				String msg = "Failed to create connection using " + source;
				throw new ResourceAcquisitionFailedException( msg, ccfe );
			}
			catch ( javax.transaction.RollbackException re )
			{
				String msg = "The transaction is marked for rollback only.";
				throw new ResourceAcquisitionFailedException( msg, re );
			}
			catch ( javax.transaction.SystemException se )
			{
				String msg = "TM encountered an unexpected error condition.";
				throw new ResourceAcquisitionFailedException( msg, se );
			}
		}
		return con;
	} // end acquireResourceConnection()
	
	
	synchronized XAResource getXaResource( byte resourceId[] )
	{
		return PersistenceSourceDispatcher.getDispatcher().
					getXaResource( resourceId );
	}
	// -- Private operations
	
	/**
	 * Releases all resources held by the transaction associated with the
	 * invoking thread. If an error occurs while attempting to release a
	 * resource, an error message is logged and the rest of the resources
	 * in the transaction will be released.
	 * @throws NotInTransactionException if the resource broker is unable to
	 * fetch a transaction for the current thread
	 */
	synchronized void releaseResourceConnectionsForTransaction()
		throws NotInTransactionException
	{
		Transaction tx = getCurrentTransaction();
		ConnectionBundle bundle = txConnectionMap.remove( tx );
		if ( bundle != null )
		{
			Iterator connections = bundle.getAllConnections();
			while ( connections.hasNext() )
			{
				try
				{
					this.destroyCon( (ResourceConnection) connections.next() );
				}
				catch ( ConnectionDestructionFailedException cdfe )
				{
					cdfe.printStackTrace();
					log.severe(	"Unable to close connection. Will continue " +
								"anyway." );
				}
			}
		}
	}
	
	/**
	 * Releases all resources held by the transaction associated with the
	 * invoking thread.
	 * @throws NotInTransactionException if the resource broker is unable to
	 * fetch a transaction for the current thread
	 */
	synchronized void delistResourcesForTransaction()
		throws NotInTransactionException
	{
		Transaction tx = this.getCurrentTransaction();
		ConnectionBundle bundle = txConnectionMap.get( tx );
		if ( bundle != null )
		{
			Iterator connections = bundle.getAllConnections();
			while ( connections.hasNext() )
			{
				ResourceConnection resourceConnection = 
					(ResourceConnection) connections.next();
				this.delistCon( tx, resourceConnection );
			}
		}
	}
	
	// Delists the XAResource that the ResourceConnection encapsulates.
	// Note that we hardcode the 'flag' to be XAResource.TMSUCCESS.
	// This code doesn't have any effect any more since resources
	// should be enlisted and delisted in xaframework implementation.
	private void delistCon( Transaction tx, ResourceConnection con )
	{
		try
		{
			tx.delistResource( con.getXAResource(), XAResource.TMSUCCESS );
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			log.severe(	"Failed to delist resource '" + con +
						"' from current transaction." );
			// TODO
			throw new RuntimeException( "replacethisexcwithsomething" );
		}
	}
	
	// Destroys the connection by invoking ResourceConnection.destroy(),
	// which is a hook for the resource manager to close() the underlying
	// connection (and, if pooling, return it to a connection pool).
	private void destroyCon( ResourceConnection con )
		throws ConnectionDestructionFailedException
	{
		con.destroy();
	}
	
	// Gets the transaction associated with the currently executing thread,
	// performs sanity checks and returns it.
	private Transaction getCurrentTransaction()
		throws NotInTransactionException
	{
		try
		{
			// Get transaction for current thread
			TransactionManager tm = TransactionFactory.getTransactionManager();
			Transaction tx = tm.getTransaction();

			// If none found, yell about it
			if ( tx == null )
			{
				throw new NotInTransactionException( "No transaction found " +
													 "for current thread" );
			}
			
			return tx;
		}
		catch ( SystemException se )
		{
			throw new NotInTransactionException( "Error fetching transaction " +
												 "for current thread", se );
		}
	}
	
	
	// -- Inner classes
	
	// A bundle of connections for a transaction. Basically, the
	// <CODE>ConnectionBundle</CODE> is a wrapped hash table that
	// maps PersistenceSources to ResourceConnections.
	private static class ConnectionBundle
	{
		private Map<PersistenceSource,ResourceConnection> sourceConnectionMap = 
			new HashMap<PersistenceSource,ResourceConnection>();
		
		boolean hasConnectionForPersistenceSource( PersistenceSource source )
		{
			return this.sourceConnectionMap.containsKey( source );
		}
		
		ResourceConnection getConnectionForPersistenceSource( PersistenceSource
															  source )
		{
			return this.sourceConnectionMap.get( source );
		}
		
		void setConnectionForPersistenceSource( PersistenceSource source,
												ResourceConnection con )
		{
			if ( this.sourceConnectionMap.containsKey(source) )
			{
				throw new RuntimeException( "There's already a connection " +
											"allocated for " + source );
			}
			this.sourceConnectionMap.put( source, con );
		}
		
		Iterator getAllConnections()
		{
			return this.sourceConnectionMap.values().iterator();
		}
	}
	
	// The transaction commit hook: invoked before and after completion
	// of a transaction. Before completion, we delist resources. After
	// completion, we notify the command pool that it should either
	// clear and release the commands (if all went well) or undo them
	// (if the commit failed). We also release the resource connections
	// we have assigned to this transaction after completion.
	private static class TxCommitHook implements Synchronization
	{
		// Delist resources. This is done automatically by some Tx manager,
		// but this behavior is not required by the JTA spec, so we delist 
		// here just to be on the safe side of things.
		public void beforeCompletion()
		{
			try
			{
				getBroker().delistResourcesForTransaction();
				releaseConnections();
			}
			catch ( Throwable t )
			{
				t.printStackTrace();
				log.severe( "Unable to delist resources for tx." );
			}
		}

		public void afterCompletion( int param )
		{
		}
		
		private void releaseConnections()
		{
			try
			{
				getBroker().releaseResourceConnectionsForTransaction();
			}
			catch ( Throwable t )
			{
				t.printStackTrace();
				log.severe( "Error while releasing resources for tx." );
			}
		}

	}
}

