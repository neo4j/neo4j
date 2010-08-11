package org.neo4j.kernel.ha;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.LockManagerFactory;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.LockResult;
import org.neo4j.kernel.impl.ha.LockStatus;
import org.neo4j.kernel.impl.ha.ResponseReceiver;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.LockManager;
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
            return new SlaveLockManager( txModule.getTxManager(), broker, receiver );
        }
    };
    
    private final Broker broker;
    private final TransactionManager tm;
    private final ResponseReceiver receiver;
    
    public SlaveLockManager( TransactionManager tm, Broker broker, ResponseReceiver receiver )
    {
        super( tm );
        this.tm = tm;
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
        Node node = resource instanceof Node ? (Node) resource : null;
        Relationship relationship = resource instanceof Relationship ?
                (Relationship) resource : null;
        if ( node == null && relationship == null )
        {
            // This is a "fake" resource, only grab the lock locally
            super.getReadLock( resource );
            return;
        }
        
        LockResult result = null;
        do
        {
            result = node != null ?
                    receiver.receive( broker.getMaster().acquireNodeReadLock(
                            receiver.getSlaveContext(), getLocalTxId(), node.getId() ) ) :
                    receiver.receive( broker.getMaster().acquireRelationshipReadLock(
                            receiver.getSlaveContext(), getLocalTxId(), relationship.getId() ) );
                        
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

    @Override
    public void getWriteLock( Object resource ) throws DeadlockDetectedException,
            IllegalResourceException
    {
        // Code copied from getReadLock. Fix!
        Node node = resource instanceof Node ? (Node) resource : null;
        Relationship relationship = resource instanceof Relationship ?
                (Relationship) resource : null;
        if ( node == null && relationship == null )
        {
            // This is a "fake" resource, only grab the lock locally
            super.getWriteLock( resource );
            return;
        }
        
        LockResult result = null;
        do
        {
            result = node != null ?
                    receiver.receive( broker.getMaster().acquireNodeWriteLock(
                            receiver.getSlaveContext(), getLocalTxId(), node.getId() ) ) :
                    receiver.receive( broker.getMaster().acquireRelationshipWriteLock(
                            receiver.getSlaveContext(), getLocalTxId(), relationship.getId() ) );
                    
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
    
    // Release lock is as usual, since when the master committs it will release
    // the locks there and then when this slave committs it will release its
    // locks as usual here.
}
