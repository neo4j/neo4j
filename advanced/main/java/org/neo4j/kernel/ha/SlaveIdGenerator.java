package org.neo4j.kernel.ha;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.neo4j.kernel.CommonFactories;
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
        private final Map<IdType, IdGenerator> generators = new HashMap<IdType, IdGenerator>();
        private final IdGeneratorFactory localFactory =
                CommonFactories.defaultIdGeneratorFactory();

        public SlaveIdGeneratorFactory( Broker broker, ResponseReceiver receiver )
        {
            this.broker = broker;
            this.receiver = receiver;
        }
        
        public IdGenerator open( String fileName, int grabSize, IdType idType, long highestIdInUse )
        {
            IdGenerator localIdGenerator = localFactory.open( fileName, grabSize,
                    idType, highestIdInUse );
            IdGenerator generator = new SlaveIdGenerator( idType, highestIdInUse, broker, receiver,
                    localIdGenerator );
            generators.put( idType, generator );
            return generator;
        }
        
        public void create( String fileName )
        {
            localFactory.create( fileName );
        }

        public IdGenerator get( IdType idType )
        {
            return generators.get( idType );
        }
    };
    
    private final Broker broker;
    private final ResponseReceiver receiver;
    private volatile long highestIdInUse;
    private volatile long defragCount;
    private final Queue<Long> idQueue = new LinkedList<Long>();
    private final IdType idType;
    private final IdGenerator localIdGenerator;

    public SlaveIdGenerator( IdType idType, long highestIdInUse, Broker broker,
            ResponseReceiver receiver, IdGenerator localIdGenerator )
    {
        this.idType = idType;
        this.highestIdInUse = highestIdInUse;
        this.broker = broker;
        this.receiver = receiver;
        this.localIdGenerator = localIdGenerator;
    }

    public void close()
    {
        this.localIdGenerator.close();
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
        updateLocalIdGenerator();
        return idQueue.poll();
    }

    private void updateLocalIdGenerator()
    {
        long localHighId = this.localIdGenerator.getHighId();
        if ( this.highestIdInUse > localHighId )
        {
            this.localIdGenerator.setHighId( this.highestIdInUse );
        }
    }

    private Long nextLocalIdOrNull()
    {
        return this.idQueue.poll();
    }

    public void setHighId( long id )
    {
        this.highestIdInUse = id;
        this.localIdGenerator.setHighId( id );
    }
    
    public long getDefragCount()
    {
        return this.defragCount;
    }
}
