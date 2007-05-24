package org.neo4j.impl.core;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventListenerAlreadyRegisteredException;
import org.neo4j.impl.event.EventListenerNotRegisteredException;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.event.ProActiveEventListener;
import org.neo4j.impl.transaction.TransactionFactory;

/**
 * o Check so the nodes connected to a created relationship are valid
 * o Make sure the relationship type used when creating a relationship is valid
 * o Make sure delete is only invoked once on a node/relationship 
 * o Make sure no changes are made to a deleted node/relationship
 * o Make sure a deleted node has no relationships
 */
class NeoConstraintsListener implements ProActiveEventListener
{
	private static Logger log = Logger.getLogger( 
		NeoConstraintsListener.class.getName() );
	private static NeoConstraintsListener listener = 
		new NeoConstraintsListener();
	
	// evaluator for each running transaction
	private Map<Thread,NeoConstraintsEvaluator> evaluators = 
		java.util.Collections.synchronizedMap( 
			new HashMap<Thread,NeoConstraintsEvaluator>() );

	private NeoConstraintsListener()
	{
	}
	
	static NeoConstraintsListener getListener()
	{
		return listener;
	}
	
	void registerEventListeners()
	{
		EventManager evtMgr = EventManager.getManager();
		try
		{
			evtMgr.registerProActiveEventListener( this, 
				Event.NODE_CREATE );
			evtMgr.registerProActiveEventListener( this, 
				Event.NODE_DELETE );
			evtMgr.registerProActiveEventListener( this, 
				Event.NODE_ADD_PROPERTY );
			evtMgr.registerProActiveEventListener( this, 
				Event.NODE_CHANGE_PROPERTY );
			evtMgr.registerProActiveEventListener( this, 
				Event.NODE_REMOVE_PROPERTY );
			evtMgr.registerProActiveEventListener( this, 
				Event.RELATIONSHIP_CREATE );
			evtMgr.registerProActiveEventListener( this, 
				Event.RELATIONSHIP_DELETE );
			evtMgr.registerProActiveEventListener( this, 
				Event.RELATIONSHIP_ADD_PROPERTY );
			evtMgr.registerProActiveEventListener( this, 
				Event.RELATIONSHIP_CHANGE_PROPERTY );
			evtMgr.registerProActiveEventListener( this, 
				Event.RELATIONSHIP_REMOVE_PROPERTY );
		}
		catch ( EventListenerNotRegisteredException e )
		{
			throw new RuntimeException( 
				"Unable to register Neo constraints event listener", e );
		}
		catch ( EventListenerAlreadyRegisteredException e )
		{
			throw new RuntimeException( 
				"Unable to register Neo constraints event listener", e );
		}
	}
	
	void unregisterEventListeners()
	{
		EventManager evtMgr = EventManager.getManager();
		try
		{
			evtMgr.unregisterProActiveEventListener( this, 
				Event.NODE_CREATE );
			evtMgr.unregisterProActiveEventListener( this, 
				Event.NODE_DELETE );
			evtMgr.unregisterProActiveEventListener( this, 
				Event.NODE_ADD_PROPERTY );
			evtMgr.unregisterProActiveEventListener( this, 
				Event.NODE_CHANGE_PROPERTY );
			evtMgr.unregisterProActiveEventListener( this, 
				Event.NODE_REMOVE_PROPERTY );
			evtMgr.unregisterProActiveEventListener( this, 
				Event.RELATIONSHIP_CREATE );
			evtMgr.unregisterProActiveEventListener( this, 
				Event.RELATIONSHIP_DELETE );
			evtMgr.unregisterProActiveEventListener( this, 
				Event.RELATIONSHIP_ADD_PROPERTY );
			evtMgr.unregisterProActiveEventListener( this, 
				Event.RELATIONSHIP_CHANGE_PROPERTY );
			evtMgr.unregisterProActiveEventListener( this, 
				Event.RELATIONSHIP_REMOVE_PROPERTY );
		}
		catch ( EventListenerNotRegisteredException e )
		{
			throw new RuntimeException( 
				"Unable to register Neo constraints event listener", e );
		}
	}

	public boolean proActiveEventReceived( Event event, EventData data )
	{
		Transaction tx = null;
		try
		{
			int status = TransactionFactory.getUserTransaction().getStatus();
			if ( status == Status.STATUS_NO_TRANSACTION || 
				status == Status.STATUS_MARKED_ROLLBACK )
			{
				// no transaction or already marked for rollback
				return false;
			}
			
			Thread currentThread = Thread.currentThread();
			NeoConstraintsEvaluator evaluator = evaluators.get( 
				currentThread );
			if ( evaluator == null )
			{
				tx = TransactionFactory.getTransactionManager().
					getTransaction();
				evaluator = new NeoConstraintsEvaluator();
				tx.registerSynchronization( evaluator );
				evaluators.put( currentThread, evaluator );
			}
			return evaluator.evaluate( event, data );
		}
		catch ( Throwable t )
		{
			log.log( Level.SEVERE, "Unable to proccess event " + event, t );
		}
		return false;
	}
	
	private static class NeoConstraintsEvaluator implements Synchronization
	{
		private Map<Integer,NodeImpl> deletedNodes = null;
		private Set<RelationshipImpl> deletedRelationships = null;
		
		NeoConstraintsEvaluator()
		{
		}
		
		boolean evaluate( Event event, EventData eventData )
		{
			if ( event == Event.NODE_CREATE )
			{
				// just for evaluator to register tx commit hook
				return true;
			}
			else if ( event == Event.RELATIONSHIP_CREATE )
			{
				RelationshipImpl rel = ( RelationshipImpl ) 
					( ( RelationshipCommands ) eventData.getData() 
						).getEntity();
				return evaluateCreateRelationship( rel );
			}
			else if ( event == Event.NODE_DELETE )
			{
				NodeImpl node = ( NodeImpl ) (( NodeCommands ) 
					eventData.getData()).getEntity();
				return evaluateDeleteNode( node );
			}
			else if ( event == Event.RELATIONSHIP_DELETE )
			{
				RelationshipImpl rel = ( RelationshipImpl ) 
					( ( RelationshipCommands ) eventData.
						getData()).getEntity();
				return evaluateDeleteRelationship( rel );
			}
			else if (	event == Event.NODE_ADD_PROPERTY || 
						event == Event.NODE_CHANGE_PROPERTY ||
						event == Event.NODE_REMOVE_PROPERTY )
			{
				// check if node deleted
				Node node = ( Node ) (( NodeCommands ) 
					eventData.getData()).getEntity();
				return evaluateNodePropertyOperation( node, event, eventData );
			}
			else if (	event == Event.RELATIONSHIP_ADD_PROPERTY ||
						event == Event.RELATIONSHIP_CHANGE_PROPERTY ||
						event == Event.RELATIONSHIP_REMOVE_PROPERTY )
			{
				// check if rel deleted
				Relationship rel = ( Relationship ) 
					( ( RelationshipCommands ) eventData.
						getData()).getEntity();
				return evaluateRelationshipPropertyOperation( rel, event, 
					eventData );
			}
			return false;
		}
	
		private boolean evaluateCreateRelationship( RelationshipImpl rel )
		{
			// verify nodes not deleted
			Integer nodeIds[] = rel.getNodeIds();
			if ( deletedNodes != null )
			{
				if ( deletedNodes.containsKey( nodeIds[0] ) )
				{
					log.severe( "Node[0] : " + nodeIds[0] + 
						", on created relationship[" + rel + "]" + 
						" does not exist (deleted in same tx)" );
					return false;
				}
				if ( deletedNodes.containsKey( nodeIds[1] ) )
				{
					log.severe( "Node[1] : " + nodeIds[1] + 
						", on created relationship[" + rel + "]" + 
						" does not exist (deleted in same tx)" );
					return false;
				}
			}
			// verify legal nodes so they aren't old references from 
			// other tx where they where delted etc.
			// but since we have write lock on them they are valid?

			// verify relationship type
			RelationshipTypeHolder rth = RelationshipTypeHolder.getHolder();
			if ( ! rth.isValidRelationshipType( rel.getType() ) )
			{
				log.severe( "Illegal relationship type[" + 
					rel.getType() + "] for created relationship[" + 
					rel + "]" );
				return false;
			}
			
			return true;
		}
	
		private boolean evaluateDeleteNode( NodeImpl node )
		{
			// we are not allowed to invoke multiple deletes on node
			if ( deletedNodes != null &&  
				deletedNodes.containsKey( (int) node.getId() ) )
			{
				log.severe( "Delete of node[" + node + 
					"] illegal since it has already been deleted (in this tx)" );
				return false;
			}
//			if ( node.getRelationships().iterator().hasNext() )
//			{
//				log.severe( "Delete of node[" + node + "] illegal since it " +
//					"still has relationships" );
//			}
				
			if ( deletedNodes == null )
			{
				deletedNodes = new java.util.HashMap<Integer,NodeImpl>();
			}
			deletedNodes.put( (int) node.getId(), node );
			return true;
		}
	
		private boolean evaluateDeleteRelationship( RelationshipImpl rel )
		{
			// we are not allowed to invoke multiple deletes on node
			if ( deletedRelationships != null && 
					deletedRelationships.contains( rel ) )
			{
				log.severe( "Delete of relationship[" + rel + 
					"] illegal since it has already been deleted (in this tx)" 
					);
				return false;
			}
			// remove this rel from deleted nodes in this tx
			// have to do this since cache won't return correct node impl 
			// if deletion is in wrong order (eg node.del then rel.del)
			Integer nodeIds[] = ( ( RelationshipImpl ) rel ).getNodeIds();
			if ( deletedNodes != null && 
					deletedNodes.containsKey( nodeIds[0] ) )
			{
				( ( NodeImpl ) deletedNodes.get( 
					nodeIds[0] ) ).removeRelationship( rel.getType(), 
						new Integer( (int) rel.getId() ) );
			}
			if ( deletedNodes != null &&
					deletedNodes.containsKey( nodeIds[1] ) )
			{
				( ( NodeImpl ) deletedNodes.get( 
					nodeIds[1] ) ).removeRelationship( rel.getType(), 
						new Integer( (int) rel.getId() ) );
			}

			if ( deletedRelationships == null )
			{
				deletedRelationships = 
					new java.util.HashSet<RelationshipImpl>();
			}
			deletedRelationships.add( rel );
			return true;
		}
		
		private boolean evaluateNodePropertyOperation( Node node, Event event, 
			EventData eventData )
		{
			if ( deletedNodes != null && 
					deletedNodes.containsKey( (int) node.getId() ) )
			{
				log.severe( "Property operation[" + event + "] on node[" + 
					node + 
					"] illegal since it has already been deleted (in this tx)" );
				return false;
			}
			// make sure it exist and that is done in Add/Change/Remove
			if ( event == Event.NODE_ADD_PROPERTY ) 
			{
				Object property = (( NodeCommands )	eventData.getData() 
					).getProperty();
				return validatePropertyType( node, property );
			}
			return true;
		}

		private boolean evaluateRelationshipPropertyOperation( 
			Relationship rel, Event event, EventData eventData )
		{
			// we are not allowed to change deleted relationship
			if ( deletedRelationships != null && 
					deletedRelationships.contains( rel ) )
			{
				log.severe( "Property operation[" + event + 
					"] on relationship[" + rel + 
					"] illegal since it has already been deleted (in this tx)" 
					);
				return false;
			}
			// make sure it exist and that is done in Add/Change/Remove 
			if ( event == Event.RELATIONSHIP_ADD_PROPERTY ) 
			{
				Object property = (( RelationshipCommands ) eventData.getData()
					).getProperty();
				return validatePropertyType( rel, property );
			}
			return true;
		}
		
		private boolean validatePropertyType( Object entity, Object prop )
		{
			if ( prop instanceof String || prop instanceof Boolean || 
				prop instanceof Byte || prop instanceof Integer || 
				prop instanceof Long || prop instanceof Float || 
				prop instanceof Double )
			{
				return true;
			}
			log.severe( "Illegal property type added to[" + entity + "]" );
			return false;
		}

		public void afterCompletion( int arg0 )
		{
			// do nothing
		}

		public void beforeCompletion()
		{
			getListener().removeThisEvaluator();
			if ( deletedNodes != null )
			{
				Iterator<NodeImpl> itr = 
					deletedNodes.values().iterator();
				while ( itr.hasNext() )
				{
					NodeImpl node = ( NodeImpl ) itr.next();
					if ( node.hasRelationships() )
					{
						log.severe( "Deleted Node[" + node + 
							"] still has relationship." );
						setRollbackOnly();
					}
				}
			}
		}

		private void setRollbackOnly()
		{
			try
			{
				TransactionFactory.getTransactionManager().setRollbackOnly();
			}
			catch ( javax.transaction.SystemException se )
			{
				log.severe( "Failed to set transaction rollback only" );
			}
		}
	}
	
	private void removeThisEvaluator()
	{
		evaluators.remove( Thread.currentThread() );
	}
}