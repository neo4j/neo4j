package org.neo4j.kernel.ha;

import javax.transaction.TransactionManager;

import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.TxManager;

public class SlaveRelationshipTypeCreator implements RelationshipTypeCreator
{
    private final Broker broker;
    private final ResponseReceiver receiver;

    public SlaveRelationshipTypeCreator( Broker broker, ResponseReceiver receiver )
    {
        this.broker = broker;
        this.receiver = receiver;
    }
    
    public int getOrCreate( TransactionManager txManager, EntityIdGenerator idGenerator,
            PersistenceManager persistence, RelationshipTypeHolder relTypeHolder, String name )
    {
        try
        {
            int eventIdentifier = ((TxManager) txManager).getEventIdentifier();
            return receiver.receive( broker.getMaster().first().createRelationshipType(
                    receiver.getSlaveContext( eventIdentifier ), name ) );
        }
        catch ( ZooKeeperException e )
        {
            receiver.newMaster( null, e );
            throw e;
        }
        catch ( HaCommunicationException e )
        {
            receiver.newMaster( null, e );
            throw e;
        }
    }
}
