package org.neo4j.impl.persistence;

// Kernel imports
import java.util.logging.Logger;

import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventListenerAlreadyRegisteredException;
import org.neo4j.impl.event.EventListenerNotRegisteredException;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.event.ProActiveEventListener;
import org.neo4j.impl.event.ReActiveEventListener;
import org.neo4j.impl.transaction.NotInTransactionException;


/**
 * The BusinessLayerMonitor is responsible for monitoring the kernel
 * business layer for changes that should be reflected in persistent state.
 * It contains much of the macro-level program flow for persistence.
 * <P>
 * The BusinessLayerMonitor listens to the events that are generated when
 * clients or other components affects the kernel business layer, analyzes
 * the events and generates the appropriate persistence commands
 * (i.e SQL code) for making them persistent.
 * <P>
 * Implementation notes:
 * <UL>
 *	<LI>The monitor listens to <I>proactive events</I>
 *		(see {@link org.neo4j.impl.event.EventManager} for a description of
 *		proactive events) for business layer state changes. It returns
 *		<CODE>false</CODE> if it fails to persist the state change.
 *	<LI>The monitor listens to <I>reactive events</I> for monitoring the
 *		addition and removal of persistence source available to the kernel.
 *	<LI>Most of the logic resides in
 *		<CODE>performPersistenceOperationForEvent()</CODE>. This method is
 *		invoked whenever an object has changed state in the business layer.
 *		It translates the event to SQL code, requests a connection for the
 *		transaction and writes the SQL to the connection.
 *	<LI>All the private register/unregister* methods at the end of this class
 *		are used only to register/unregister the BusinessLayerMonitor on events
 *		whenever the persistence module starts up and shuts down. Adding batch
 *		registration methods to the event framework would clean out most of
 *		that code, but for now it's a necessary mess.
 * </UL>
 */
class BusinessLayerMonitor implements	ProActiveEventListener,
										ReActiveEventListener
{
	Logger log = Logger.getLogger( BusinessLayerMonitor.class.getName() );
	
	// -- Singleton stuff
	private static BusinessLayerMonitor monitor = new BusinessLayerMonitor();
	private BusinessLayerMonitor() {}
	/**
	 * Singleton accessor.
	 * @return the singleton business layer monitor
	 */
	static BusinessLayerMonitor getMonitor()
	{
		return monitor;
	}
	
	
	// -- Event hooks

	// javadoc: see ProActiveEventListener.proActiveEventReceived
	public boolean proActiveEventReceived( Event event, EventData data )
	{
		boolean success = false;
		if ( event == Event.DATA_SOURCE_ADDED )
		{
			PersistenceSourceDispatcher.getDispatcher().
				persistenceSourceAdded( (PersistenceSource) data.getData() );
			return true;
		}
		try
		{
			this.performPersistenceOperationForEvent( event, data );
			success = true;
		}
		catch ( IllegalArgumentException iae )
		{
			iae.printStackTrace();
			log.severe(	"Unable to persist the operation represented by " +
						event + " with data '" + data + "' due to bad " +
						"arguments." );
		}
		catch ( UnsupportedPersistenceTypeException upte )
		{
			upte.printStackTrace();
			log.severe(	"Unable to persist the operation represented by " +
						event + " with data '" + data + "' because a " +
						"component reported that the persistence type was " +
						"unsupported." );
		}
		catch ( ResourceAcquisitionFailedException rafe )
		{
			rafe.printStackTrace();
			log.severe(	"Unable to persist the operation represented by " +
						event + " with data '" + data + "' because we " +
						"were unable to acquire a resource connection for " +
						"the operation." );
		}
		catch ( NotInTransactionException nite )
		{
			nite.printStackTrace();
			log.warning(	"Unable to persist the operation represented by " +
						event + " with data '" + data + "' because the " +
						"current thread is not participating in a " +
						"transaction." );
		}
		catch ( PersistenceUpdateFailedException pufe )
		{
			pufe.printStackTrace();
			log.severe(	"Unable to persist the operation represented by " +
						event + " with data '" + data + "' because " +
						"something went wrong when attempting to update " +
						"the persistence source." );
			
		}
		catch ( Throwable t )
		{
			t.printStackTrace();
			log.severe(	"Caught an unknown exception while attempting to " +
						"persist " + event + " with data '" + data + ".' " +
						"Operation not persisted." );
		}
		return success;
	}
	
	// javadoc: see ReActiveEventListener.reActiveEventReceived()
	// on DATA_SOURCE_ADDED		->	notify PersistenceSourceDispatcher that
	//								a new persistence source has been hooked
	//								up to the kernel
	// on DATA_SOURCE_REMOVED	->	notify PersistenceSourceDispatcher that
	//								a persistence source has been removed from
	//								the kernel
	// everything else			->	report error
	public void reActiveEventReceived( Event event, EventData data )
	{
		// Sanity check: event data must not be null, and data.getData() must
		// implement PersistenceSource. This is true for the events
		// DATA_SOURCE_ADDED and _REMOVED, but may change in the future.
		if ( data == null || !(data.getData() instanceof PersistenceSource) )
		{
			String msg = "The data for " + event + " is " + data +
						 ", which does not implement PersistenceSource.";
			throw new IllegalArgumentException(	msg );
		}

		/*if ( event == Event.DATA_SOURCE_ADDED )
		{
			PersistenceSourceDispatcher.getDispatcher().
				persistenceSourceAdded( (PersistenceSource) data.getData() );
		}
		else*/ if ( event == Event.DATA_SOURCE_REMOVED )
		{
			PersistenceSourceDispatcher.getDispatcher().
				persistenceSourceRemoved( (PersistenceSource) data.getData() );
		}
		else
		{
			log.severe("Received event " + event + ", of which I know nothing.");
		}
	}
	

	// -- Persistence-related operations

	// Execute the sql required to make the operation represented by
	// 'event' persistent. If we don't know how to persist 'event,'
	// or 'data' is malformed, an IllegalArgumentException is raised.
	private void performPersistenceOperationForEvent( Event event,
													  EventData data )
		throws	UnsupportedPersistenceTypeException,
				ResourceAcquisitionFailedException,
				NotInTransactionException,
				PersistenceUpdateFailedException
	{
		if ( data == null || !(data.getData() instanceof PersistenceMetadata) )
		{
			String msg = "The data for " + event + " is " + data +
						 ", which does not implement PersistenceMetadata.";
			throw new IllegalArgumentException(	msg );
		}
		PersistenceMetadata	entityMetaData 	= 
			(PersistenceMetadata) data.getData();
		ResourceConnection resource = 
			ResourceBroker.getBroker().acquireResourceConnection( 
				entityMetaData );
		resource.performUpdate( event, data );
	}
	
	// -- Lifecycle operations

	/**
	 * Registers the monitor as a listener for the appropriate events.
	 * Invoked by the PersistenceModule on startup- and reload requests.
	 * If the monitor is already registered for an event, it will silently
	 * continue.
	 * @throws EventListenerNotRegisteredException if event registration of any
	 * event failed
	 */
	void registerEvents()
		throws	EventListenerNotRegisteredException
	{
		this.registerProActiveEvent( Event.NODE_CREATE );
		this.registerProActiveEvent( Event.NODE_DELETE );
		this.registerProActiveEvent( Event.NODE_ADD_PROPERTY );
		this.registerProActiveEvent( Event.NODE_REMOVE_PROPERTY );
		this.registerProActiveEvent( Event.NODE_CHANGE_PROPERTY );

		this.registerProActiveEvent( Event.RELATIONSHIP_CREATE );
		this.registerProActiveEvent( Event.RELATIONSHIP_DELETE );
		this.registerProActiveEvent( Event.RELATIONSHIP_ADD_PROPERTY );
		this.registerProActiveEvent( Event.RELATIONSHIP_REMOVE_PROPERTY );
		this.registerProActiveEvent( Event.RELATIONSHIP_CHANGE_PROPERTY );
		this.registerProActiveEvent( Event.RELATIONSHIPTYPE_CREATE );
		this.registerProActiveEvent( Event.DATA_SOURCE_ADDED );
		
		// this.registerReActiveEvent( Event.DATA_SOURCE_ADDED );
		this.registerReActiveEvent( Event.DATA_SOURCE_REMOVED );
	}
	
	/**
	 * Unregisters the monitor as a listener for events. This method is
	 * by the persistence module on stop- and reload requests. It does
	 * not raise any exceptions, but rather fails silently (with a
	 * message in the logs) as it should.
	 */
	void unregisterEvents()
	{
		this.unregisterProActiveEvent( Event.NODE_CREATE );
		this.unregisterProActiveEvent( Event.NODE_DELETE );
		this.unregisterProActiveEvent( Event.NODE_ADD_PROPERTY );
		this.unregisterProActiveEvent( Event.NODE_REMOVE_PROPERTY );
		this.unregisterProActiveEvent( Event.NODE_CHANGE_PROPERTY );
		
		this.unregisterProActiveEvent( Event.RELATIONSHIP_CREATE );
		this.unregisterProActiveEvent( Event.RELATIONSHIP_DELETE );
		this.unregisterProActiveEvent( Event.RELATIONSHIP_ADD_PROPERTY );
		this.unregisterProActiveEvent( Event.RELATIONSHIP_REMOVE_PROPERTY );
		this.unregisterProActiveEvent( Event.RELATIONSHIP_CHANGE_PROPERTY );
		this.unregisterProActiveEvent( Event.RELATIONSHIPTYPE_CREATE );
		this.unregisterProActiveEvent( Event.DATA_SOURCE_ADDED );
		
		// this.unregisterReActiveEvent( Event.DATA_SOURCE_ADDED );
		this.unregisterReActiveEvent( Event.DATA_SOURCE_REMOVED );
	}
	
	
	// Registers the business layer monitor as a proactive event listener
	// for events of type 'event.'
	private void registerProActiveEvent( Event event )
		throws EventListenerNotRegisteredException
	{
		try
		{
			EventManager.getManager().registerProActiveEventListener(	this,
																		event );
		}
		catch ( EventListenerAlreadyRegisteredException elare )
		{
			// This is no big deal, we've just forgotten to clean up after
			// ourselves. Log and go on.
			log.warning( "Failed to register ourselves as a listener on " +
					  event + " events because we are already registered." );
		}
	}
	
	// Registers the business layer monitor as a reactive event listener
	// for events of type 'event'.
	private void registerReActiveEvent( Event event )
		throws EventListenerNotRegisteredException
	{
		try
		{
			EventManager.getManager().registerReActiveEventListener(	this,
																		event );
		}
		catch ( EventListenerAlreadyRegisteredException elare )
		{
			// This is no big deal, we've just forgotten to clean up after
			// ourselves. Log and go on.
			log.warning( "Failed to register ourselves as a listener on " +
					  event + " events because we are already registered." );
		}
	}
	
	// Unregisters the business layer monitor as a proactive event listener
	// for events of type 'event.'
	private void unregisterProActiveEvent( Event event )
	{
		try
		{
			EventManager.getManager().unregisterProActiveEventListener(	this,
																		event );
		}
		catch ( EventListenerNotRegisteredException elnre )
		{
			elnre.printStackTrace();
			// We should be registered for these events. If not, something
			// probably messed up during startup. Drop a message in the log
			// and move on.
			log.warning( "Failed to unregister ourselves as a listener on " +
					  event + " events because we weren't registered." );
		}
	}
	
	// Unregisters the business layer monitor as a reactive event listener
	// for events of type 'event.'
	private void unregisterReActiveEvent( Event event )
	{
		try
		{
			EventManager.getManager().unregisterReActiveEventListener(	this,
																		event );
		}
		catch ( EventListenerNotRegisteredException elnre )
		{
			elnre.printStackTrace();
			// We should be registered for these events. If not, something
			// probably messed up during startup. Drop a message in the log
			// and move on.
			log.warning( "Failed to unregister ourselves as a listener on " +
					  event + " events because we weren't registered." );
		}
	}
}
