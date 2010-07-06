package org.neo4j.kernel.ha;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.ha.Broker;
import org.neo4j.kernel.impl.ha.IdAllocation;
import org.neo4j.kernel.impl.ha.ResponseReceiver;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;

public class SlaveIdGenerator implements IdGenerator
{
    public static class SlaveIdGeneratorFactory implements IdGeneratorFactory
    {
        private final Broker broker;
        private final ResponseReceiver receiver;

        public SlaveIdGeneratorFactory( Broker broker, ResponseReceiver receiver )
        {
            this.broker = broker;
            this.receiver = receiver;
        }
        
        public IdGenerator open( String fileName, int grabSize, IdType idType, long highestIdInUse )
        {
            return new SlaveIdGenerator( idType, highestIdInUse, broker, receiver );
        }
        
        public void create( String fileName )
        {
        }
    };
    
    private final Broker broker;
    private final ResponseReceiver receiver;
    private volatile long highestIdInUse;
    private volatile int defragCount;
    private final Queue<Long> idQueue = new LinkedList<Long>();
    private final IdType idType;

    public SlaveIdGenerator( IdType idType, long highestIdInUse, Broker broker,
            ResponseReceiver receiver )
    {
        this.idType = idType;
        this.highestIdInUse = highestIdInUse;
        this.broker = broker;
        this.receiver = receiver;
    }

    public void close()
    {
    }

    public void freeId( long id )
    {
    }

    public long getHighId()
    {
        return this.highestIdInUse;
    }

    public long getNumberOfIdsInUse()
    {
        return this.highestIdInUse - this.defragCount;
    }

    public synchronized long nextId()
    {
        // if we dont have anymore grabbed ids from master, grab a bunch 
        Long nextId = nextLocalIdOrNull();
        if ( nextId == null )
        {
            IdAllocation allocation = receiver.receive(
                    broker.getMaster().allocateIds( broker.getSlaveContext(), idType ) );
            nextId = storeLocally( allocation );
        }
        return nextId.intValue();
    }

    private Long storeLocally( IdAllocation allocation )
    {
        this.highestIdInUse = allocation.getHighestIdInUse();
        this.defragCount = allocation.getDefragCount();
        for ( long id : allocation.getIds() )
        {
            idQueue.add( id );
        }
        return idQueue.poll();
    }

    private Long nextLocalIdOrNull()
    {
        return this.idQueue.poll();
    }

    public void setHighId( long id )
    {
        this.highestIdInUse = id;
    }
}
