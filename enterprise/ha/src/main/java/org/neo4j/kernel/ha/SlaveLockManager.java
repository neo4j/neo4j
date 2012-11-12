/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.ha;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.com.ComException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.LockManagerFactory;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.TxModule;

public class SlaveLockManager extends LockManager
{
    public static class SlaveLockManagerFactory implements LockManagerFactory
    {
        private final Broker broker;
        private final ResponseReceiver receiver;

        public SlaveLockManagerFactory( Broker broker, ResponseReceiver receiver )
        {
            this.broker = broker;
            this.receiver = receiver;
        }
        
        public LockManager create( TxModule txModule )
        {
            return new SlaveLockManager( txModule.getTxManager(), txModule.getTxHook(), broker, receiver );
        }
    };
    
    private final Broker broker;
    private final TransactionManager tm;
    private final ResponseReceiver receiver;
    private final TxHook txHook;
    
    public SlaveLockManager( TransactionManager tm, TxHook txHook, Broker broker, ResponseReceiver receiver )
    {
        super( tm );
        this.tm = tm;
        this.txHook = txHook;
        this.broker = broker;
        this.receiver = receiver;
    }

    private int getLocalTxId()
    {
        return ((TxManager) tm).getEventIdentifier();
    }
    
    @Override
    public void getReadLock( Object resource ) throws DeadlockDetectedException,
            IllegalResourceException
    {
        try
        {
            Node node = resource instanceof Node ? (Node) resource : null;
            Relationship relationship = resource instanceof Relationship ?
                    (Relationship) resource : null;
            if ( node == null && relationship == null )
            {
                // This is a "fake" resource, only grab the lock locally
                super.getReadLock( resource );
                return;
            }
            
//            if ( hasAlreadyGotLock() )
//            {
//                return;
//            }
            
            initializeTxIfFirst();
            LockResult result = null;
            do
            {
                int eventIdentifier = getLocalTxId();
                result = node != null ?
                        receiver.receive( broker.getMaster().first().acquireNodeReadLock(
                                receiver.getSlaveContext( eventIdentifier ), node.getId() ) ) :
                        receiver.receive( broker.getMaster().first().acquireRelationshipReadLock(
                                receiver.getSlaveContext( eventIdentifier ), relationship.getId() ) );
                            
                switch ( result.getStatus() )
                {
                case OK_LOCKED:
                    super.getReadLock( resource );
                    return;
                case DEAD_LOCKED:
                    throw new DeadlockDetectedException( result.getDeadlockMessage() );
                }
            }
            while ( result.getStatus() == LockStatus.NOT_LOCKED );
        }
        catch ( ZooKeeperException e )
        {
            receiver.newMaster( e );
            throw e;
        }
        catch ( ComException e )
        {
            receiver.newMaster( e );
            throw e;
        }
    }

    private void initializeTxIfFirst()
    {
        // The main point of initializing transaction (for HA) is in TransactionImpl, so this is
        // for that extra point where grabbing a lock
        try
        {
            Transaction tx = tm.getTransaction();
            if ( !txHook.hasAnyLocks( tx ) ) txHook.initializeTransaction( ((TxManager)tm).getEventIdentifier() );
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void getWriteLock( Object resource ) throws DeadlockDetectedException,
            IllegalResourceException
    {
        // Code copied from getReadLock. Fix!
        try
        {
            Node node = resource instanceof Node ? (Node) resource : null;
            Relationship relationship = resource instanceof Relationship ?
                    (Relationship) resource : null;
            if ( node == null && relationship == null )
            {
                // This is a "fake" resource, only grab the lock locally
                super.getWriteLock( resource );
                return;
            }
            
//          if ( hasAlreadyGotLock() )
//          {
//              return;
//          }
            
            initializeTxIfFirst();
            LockResult result = null;
            do
            {
                int eventIdentifier = getLocalTxId();
                result = node != null ?
                        receiver.receive( broker.getMaster().first().acquireNodeWriteLock(
                                receiver.getSlaveContext( eventIdentifier ), node.getId() ) ) :
                        receiver.receive( broker.getMaster().first().acquireRelationshipWriteLock(
                                receiver.getSlaveContext( eventIdentifier ), relationship.getId() ) );
                        
                switch ( result.getStatus() )
                {
                case OK_LOCKED:
                    super.getWriteLock( resource );
                    return;
                case DEAD_LOCKED:
                    throw new DeadlockDetectedException( result.getDeadlockMessage() );
                }
            }
            while ( result.getStatus() == LockStatus.NOT_LOCKED );
        }
        catch ( ZooKeeperException e )
        {
            receiver.newMaster( e );
            throw e;
        }
        catch ( ComException e )
        {
            receiver.newMaster( e );
            throw e;
        }
    }
    
    // Release lock is as usual, since when the master committs it will release
    // the locks there and then when this slave committs it will release its
    // locks as usual here.
}
