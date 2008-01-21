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
package org.neo4j.impl.core;


import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.neo4j.impl.transaction.NotInTransactionException;
import org.neo4j.impl.util.ArrayMap;

/**
 * o Check so the nodes connected to a created relationship are valid
 * o Make sure the relationship type used when creating a relationship is valid
 * o Make sure delete is only invoked once on a node/relationship 
 * o Make sure no changes are made to a deleted node/relationship
 * o Make sure a deleted node has no relationships
 */
class NeoConstraintsListener // implements ProActiveEventListener
{
	static Logger log = Logger.getLogger( 
		NeoConstraintsListener.class.getName() );
	
	// evaluator for each running transaction
	private final ArrayMap<Thread,NeoConstraintsEvaluator> evaluators = 
		new ArrayMap<Thread,NeoConstraintsEvaluator>( 5, true, true );

	private final TransactionManager transactionManager;
	
	NeoConstraintsListener( TransactionManager transactionManager )
	{
		this.transactionManager = transactionManager;
	}

	private class NeoConstraintsEvaluator implements Synchronization
	{
		private Map<Integer,NodeImpl> deletedNodes = null;
		private Set<Integer> deletedRelationships = null;
		
		NeoConstraintsEvaluator()
		{
		}
		
		public void afterCompletion( int arg0 )
		{
			// do nothing
		}

		public void beforeCompletion()
		{
			// TODO: if we get called when status active
			// and transaction is beeing rolled back this may not evaluate
			// no harm done but will get the "severe" log message
			try
			{
			removeThisEvaluator();
			if ( getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK )
			{
				// no need to evaluate
				return;
			}
			if ( deletedNodes != null )
			{
				Iterator<NodeImpl> itr = 
					deletedNodes.values().iterator();
				while ( itr.hasNext() )
				{
					NodeImpl node = itr.next();
					if ( node.internalHasRelationships() )
					{
						log.severe( "Deleted Node[" + node + 
							"] still has relationship." );
						StringBuffer buf = new StringBuffer( 
							"Found relationships " );
/*						int count = 0;
						for ( Relationship rel : node.getRelationships() )
						{
							if ( count == 10 )
							{
								buf.append( " and more..." );
								break;
							}
							buf.append(  rel.getType() ).append( " " ); 
						}*/
						log.severe( buf.toString() );
						setRollbackOnly();
					}
				}
			}
			}
			catch ( Throwable t )
			{
				t.printStackTrace();
			}
		}

		private void setRollbackOnly()
		{
			try
			{
				transactionManager.setRollbackOnly();
			}
			catch ( javax.transaction.SystemException se )
			{
				log.severe( "Failed to set transaction rollback only" );
			}
		}

		private int getTransactionStatus()
		{
			try
			{
				return transactionManager.getStatus();
			}
			catch ( javax.transaction.SystemException se )
			{
				log.severe( "Failed to set transaction rollback only" );
			}
			return -1;
		}

        void evaluateDeleteNode( NodeImpl node )
        {
            if ( deletedNodes == null )
            {
                deletedNodes = new java.util.HashMap<Integer,NodeImpl>();
            }
            if ( deletedNodes.put( (int) node.getId(), node ) != null )
            {
                throw new IllegalStateException( "Unable to delete " + node + 
                    " since it has already been deleted in this transaction." );
            }
        }
        
        void evaluateNodeNotDeleted( int nodeId )
        {
            if ( deletedNodes != null && 
                deletedNodes.containsKey( nodeId ) )
            {
                throw new IllegalStateException( "Property operation on node[" + 
                    nodeId + "] illegal since it has already been deleted " + 
                    "(in this tx)" );
            }
        }

        boolean nodeIsDeleted( int nodeId )
        {
            if ( deletedNodes != null && 
                deletedNodes.containsKey( nodeId ) )
            {
                return true;
            }
            return false;
        }
		
        void evaluateDeleteRelationship( RelationshipImpl rel )
        {
            if ( deletedRelationships == null )
            {
                deletedRelationships = 
                    new java.util.HashSet<Integer>();
            }
            if ( !deletedRelationships.add( (int) rel.getId() ) )
            {
                throw new IllegalStateException( "Unable to delete " + rel + 
                " since it has already been deleted in this transaction." );
            }
        }
        
        void evaluateRelNotDeleted( int relId )
        {
            if ( deletedRelationships != null && 
                deletedRelationships.contains( relId ) )
            {
                throw new IllegalStateException( "Property operation on rel[" + 
                    relId + "] illegal since it has already been deleted " + 
                    "(in this tx)" );
            }
        }

        boolean relIsDeleted( int relId )
        {
            if ( deletedRelationships != null && 
                deletedRelationships.contains( relId ) )
            {
                return true;
            }
            return false;
        }
        
        NodeImpl getDeletedNode( int nodeId )
        {
            return deletedNodes.get( nodeId );
        }
	}
	
	void removeThisEvaluator()
	{
		evaluators.remove( Thread.currentThread() );
	}

    private NeoConstraintsEvaluator getEvaluator()
    {
        Transaction tx = null;
        try
        {
            int status = transactionManager.getStatus();
            if ( status == Status.STATUS_NO_TRANSACTION /* || 
                status == Status.STATUS_MARKED_ROLLBACK */ )
            {
                // no transaction or already marked for rollback
                // return null;
                throw new NotInTransactionException( 
                    "Neo API calls must be done in a transaction." );
            }
            
            Thread currentThread = Thread.currentThread();
            NeoConstraintsEvaluator evaluator = evaluators.get( 
                currentThread );
            if ( evaluator == null )
            {
                tx = transactionManager.getTransaction();
                evaluator = new NeoConstraintsEvaluator();
                tx.registerSynchronization( evaluator );
                evaluators.put( currentThread, evaluator );
            }
            return evaluator;
        }
        catch ( SystemException e )
        {
            // TODO Auto-generated catch block
            throw new RuntimeException( e );
        }
        catch ( IllegalStateException e )
        {
            // TODO Auto-generated catch block
            throw new RuntimeException( e );
        }
        catch ( RollbackException e )
        {
            // TODO Auto-generated catch block
            throw new RuntimeException( e );
        }
    }
    
    void deleteNode( NodeImpl node )
    {
        getEvaluator().evaluateDeleteNode( node );
    }

    void nodePropertyOperation( int nodeId )
    {
        getEvaluator().evaluateNodeNotDeleted( nodeId );
    }

    boolean nodeIsDeleted( int nodeId )
    {
        return getEvaluator().nodeIsDeleted( nodeId );
    }

    void deleteRelationship( RelationshipImpl rel )
    {
        getEvaluator().evaluateDeleteRelationship( rel );
    }
    
    void relPropertyOperation( int relId )
    {
        getEvaluator().evaluateRelNotDeleted( relId );
    }

    boolean relIsDeleted( int relId )
    {
        return getEvaluator().relIsDeleted( relId );
    }

    NodeImpl getDeletedNode( int nodeId )
    {
        return getEvaluator().getDeletedNode( nodeId );
    }
}