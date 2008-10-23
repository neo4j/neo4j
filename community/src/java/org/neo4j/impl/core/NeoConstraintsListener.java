/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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

import org.neo4j.api.core.NotInTransactionException;
import org.neo4j.impl.util.ArrayMap;


// Check so the nodes connected to a created relationship are valid 
// Make sure the relationship type used when creating a relationship is valid 
// Make sure delete is only invoked once on a node/relationship 
// Make sure no changes are made to a deleted node/relationship 
// Make sure a deleted node has no relationships 

class NeoConstraintsListener
{
    static Logger log = Logger.getLogger( NeoConstraintsListener.class
        .getName() );

    // evaluator for each running transaction
    private final ArrayMap<Transaction,NeoConstraintsEvaluator> evaluators = 
        new ArrayMap<Transaction,NeoConstraintsEvaluator>( 5, true, true );

    private final TransactionManager transactionManager;

    NeoConstraintsListener( TransactionManager transactionManager )
    {
        this.transactionManager = transactionManager;
    }

    private class NeoConstraintsEvaluator implements Synchronization
    {
        private Map<Integer,NodeImpl> deletedNodes = null;
        private Set<Integer> deletedRelationships = null;
        private final Transaction tx;

        NeoConstraintsEvaluator( Transaction tx )
        {
            this.tx = tx;
        }

        public void afterCompletion( int arg0 )
        {
            // do nothing
        }

        public void beforeCompletion()
        {
            try
            {
                removeThisEvaluator( tx );
                if ( getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK )
                {
                    // no need to evaluate
                    return;
                }
                if ( deletedNodes != null )
                {
                    Iterator<NodeImpl> itr = deletedNodes.values().iterator();
                    while ( itr.hasNext() )
                    {
                        NodeImpl node = itr.next();
                        Map<String, Integer> leftoverRelationships =
                            node.internalGetRelationships();
                        if ( leftoverRelationships != null )
                        {
                            log.severe( "Deleted Node[" + node + 
                                "] still has relationships:\n" +
                                sumRelationships( leftoverRelationships ) );
                            StringBuffer buf = new StringBuffer(
                                "Found relationships " );
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
        
        private String sumRelationships( Map<String, Integer> rels )
        {
            StringBuffer buffer = new StringBuffer();
            int count = 0;
            for ( Map.Entry<String, Integer> entry : rels.entrySet() )
            {
                if ( count++ > 0 )
                {
                    buffer.append( "\n" );
                }
                buffer.append( "\t" + entry.getValue() + "\t" +
                    entry.getKey() );
            }
            return buffer.toString();
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
            if ( deletedNodes != null && deletedNodes.containsKey( nodeId ) )
            {
                throw new IllegalStateException( "Property operation on node[" +
                    nodeId + "] illegal since it has already been deleted " + 
                    "(in this tx)" );
            }
        }

        boolean nodeIsDeleted( int nodeId )
        {
            if ( deletedNodes != null && deletedNodes.containsKey( nodeId ) )
            {
                return true;
            }
            return false;
        }

        void evaluateDeleteRelationship( RelationshipImpl rel )
        {
            if ( deletedRelationships == null )
            {
                deletedRelationships = new java.util.HashSet<Integer>();
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
            if ( deletedNodes != null )
            {
                return deletedNodes.get( nodeId );
            }
            return null;
        }
    }

    void removeThisEvaluator( Transaction tx )
    {
        evaluators.remove( tx );
    }

    private NeoConstraintsEvaluator getEvaluator()
    {
        Transaction tx = null;
        try
        {
            int status = transactionManager.getStatus();
            if ( status == Status.STATUS_NO_TRANSACTION )
            {
                throw new NotInTransactionException(
                    "Neo API calls must be done in a transaction." );
            }

            tx = transactionManager.getTransaction();
            NeoConstraintsEvaluator evaluator = evaluators.get( tx );
            if ( evaluator == null )
            {
                evaluator = new NeoConstraintsEvaluator( tx );
                tx.registerSynchronization( evaluator );
                evaluators.put( tx, evaluator );
            }
            return evaluator;
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
        catch ( IllegalStateException e )
        {
            throw new RuntimeException( e );
        }
        catch ( RollbackException e )
        {
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