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

import java.util.EnumMap;
import java.util.Map;

import org.neo4j.com.Response;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdRange;

public class HaIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<IdType, HaIdGenerator> generators =
            new EnumMap<IdType, HaIdGenerator>( IdType.class );
    private final IdGeneratorFactory localFactory = new DefaultIdGeneratorFactory();
    private final Master master;

    public HaIdGeneratorFactory( Master master, HighAvailability highAvailability )
    {
        this.master = master;
        highAvailability.addHighAvailabilityMemberListener( new HaIdGeneratorFactoryClusterMemberListener() );
    }

    @Override
    public IdGenerator open( FileSystemAbstraction fs, String fileName, int grabSize, IdType idType )
    {
        IdGenerator initialIdGenerator = localFactory.open( fs, fileName, grabSize, idType );
        HaIdGenerator haIdGenerator = new HaIdGenerator( initialIdGenerator, fs, fileName, grabSize, idType );
        generators.put( idType, haIdGenerator );
        return haIdGenerator;
    }

    @Override
    public void create( FileSystemAbstraction fs, String fileName, long highId )
    {
        localFactory.create( fs, fileName, highId );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return generators.get( idType );
    }

    private class HaIdGeneratorFactoryClusterMemberListener implements HighAvailabilityMemberListener
    {
        @Override
        public void masterIsElected( HighAvailabilityMemberChangeEvent event )
        {
            if ( event.getNewState() == event.getOldState() )
            {
                return;
            }
            for ( HaIdGenerator generator : generators.values() )
            {
                generator.stateChanged( event );
            }
        }

        @Override
        public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            if ( event.getNewState() == event.getOldState() )
            {
                return;
            }
            for ( HaIdGenerator generator : generators.values() )
            {
                generator.stateChanged( event );
            }
        }

        @Override
        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
        {
            if ( event.getNewState() == event.getOldState() )
            {
                return;
            }
            for ( HaIdGenerator generator : generators.values() )
            {
                generator.stateChanged( event );
            }
        }

        @Override
        public void instanceStops( HighAvailabilityMemberChangeEvent event )
        {
            if ( event.getNewState() == event.getOldState() )
            {
                return;
            }
            for ( HaIdGenerator generator : generators.values() )
            {
                generator.stateChanged( event );
            }
        }
    }

    private static final long VALUE_REPRESENTING_NULL = -1;

    private enum IdGeneratorState
    {
        TBD, SLAVE, MASTER;
    }

    private class HaIdGenerator implements IdGenerator
    {
        private IdGenerator delegate;
        private final FileSystemAbstraction fs;
        private final String fileName;
        private final int grabSize;
        private final IdType idType;
        private volatile IdGeneratorState state = IdGeneratorState.TBD;

        HaIdGenerator( IdGenerator initialDelegate, FileSystemAbstraction fs, String fileName, int grabSize,
                       IdType idType )
        {
            delegate = initialDelegate;
            this.fs = fs;
            this.fileName = fileName;
            this.grabSize = grabSize;
            this.idType = idType;
        }

        /*
         * I know what you're thinking. You're thinking "shouldn't this be a ClusterInstanceListener instead". To tell
         * you the truth, with all the stuff happening around, i'm not even sure myself. But given that getting the
         * state transition here wrong will result in store corruption, the question you should be asking yourself is
         * "do I feel lucky?". So, do you feel lucky, punk?
         *
         * State transitioning for IdGenerators cannot be done with the AbstractModeSwitcher as they are now without
         * severely violating the expected contract. State has to be kept because the actions on transitions depend
         * not only on the member state we are moving to but also the previous state of the IdGenerator - if it
          * was a slave or a master IdGenerator. For that reason, it is implemented completely separately.
         */
        public void stateChanged( HighAvailabilityMemberChangeEvent event )
        {
            // Assume blockade is up and no active threads are running here

            if ( event.getNewState() == HighAvailabilityMemberState.PENDING
                    || event.getNewState() == HighAvailabilityMemberState.MASTER )
            {
                return;
            }
            long highId = delegate.getHighId();
            if ( event.getNewState() == HighAvailabilityMemberState.TO_MASTER )
            {
                if ( state == IdGeneratorState.SLAVE )
                {
                    delegate.close();
                    if ( !fs.fileExists( fileName ) )
                    {
                        localFactory.create( fs, fileName, highId );
                    }
                    delegate = localFactory.open( fs, fileName, grabSize, idType );
                }
                // Otherwise we're master or TBD (initial state) which is the same
                state = IdGeneratorState.MASTER;
            }
            else if ( event.getNewState() == HighAvailabilityMemberState.TO_SLAVE
                    || event.getNewState() == HighAvailabilityMemberState.SLAVE )
            {

                if ( state == IdGeneratorState.SLAVE )
                {
                    // I'm already slave, just forget about ids from the previous master
                    ((SlaveIdGenerator) delegate).forgetIdAllocationFromMaster( master );
                }
                else
                {
                    delegate.close();
                    delegate.delete();
                    delegate = new SlaveIdGenerator( idType, highId, master );
                }
                state = IdGeneratorState.SLAVE;
            }
        }

        public String toString()
        {
            return delegate.toString();
        }

        public final boolean equals( Object other )
        {
            return delegate.equals( other );
        }

        public final int hashCode()
        {
            return delegate.hashCode();
        }

        public long nextId()
        {
            return delegate.nextId();
        }

        public IdRange nextIdBatch( int size )
        {
            return delegate.nextIdBatch( size );
        }

        public void setHighId( long id )
        {
            delegate.setHighId( id );
        }

        public long getHighId()
        {
            return delegate.getHighId();
        }

        public void freeId( long id )
        {
            delegate.freeId( id );
        }

        public void close()
        {
            delegate.close();
        }

        public long getNumberOfIdsInUse()
        {
            return delegate.getNumberOfIdsInUse();
        }

        public long getDefragCount()
        {
            return delegate.getDefragCount();
        }

        public void delete()
        {
            delegate.delete();
        }
    }

    private static class SlaveIdGenerator implements IdGenerator
    {
        private volatile long highestIdInUse;
        private volatile long defragCount;
        private volatile IdRangeIterator idQueue = EMPTY_ID_RANGE_ITERATOR;
        private volatile Master master;
        private final IdType idType;

        SlaveIdGenerator( IdType idType, long highId, Master master )
        {
            this.idType = idType;
            this.highestIdInUse = highId;
            this.master = master;
        }

        void forgetIdAllocationFromMaster( Master master )
        {
            if ( this.master.equals( master ) )
            {
                return;
            }

            this.idQueue = EMPTY_ID_RANGE_ITERATOR;
            this.master = master;
        }

        @Override
        public void close()
        {
        }

        public void freeId( long id )
        {
        }

        public long getHighId()
        {
            return highestIdInUse;
        }

        public long getNumberOfIdsInUse()
        {
            return highestIdInUse - defragCount;
        }

        public synchronized long nextId()
        {
            long nextId = nextLocalId();
            if ( nextId == VALUE_REPRESENTING_NULL )
            {
                // If we dont have anymore grabbed ids from master, grab a bunch
                Response<IdAllocation> response = master.allocateIds( idType );
                IdAllocation allocation = response.response();
                response.close();
                nextId = storeLocally( allocation );
            }
            // TODO necessary check?
//            else if ( !master.equals( stuff.getMaster() ) )
//                throw new ComException( "Master changed" );
            return nextId;
        }

        public IdRange nextIdBatch( int size )
        {
            throw new UnsupportedOperationException( "Should never be called" );
        }

        private long storeLocally( IdAllocation allocation )
        {
            this.highestIdInUse = allocation.getHighestIdInUse();
            this.defragCount = allocation.getDefragCount();
            this.idQueue = new IdRangeIterator( allocation.getIdRange() );
            return idQueue.next();
        }

        private long nextLocalId()
        {
            return this.idQueue.next();
        }

        public void setHighId( long id )
        {
            // TODO Check for if it's lower than what I have?
            this.highestIdInUse = id;
        }

        public long getDefragCount()
        {
            return this.defragCount;
        }

        @Override
        public void delete()
        {
        }
    }

    private static class IdRangeIterator
    {
        private int position = 0;
        private final long[] defrag;
        private final long start;
        private final int length;

        IdRangeIterator( IdRange idRange )
        {
            this.defrag = idRange.getDefragIds();
            this.start = idRange.getRangeStart();
            this.length = idRange.getRangeLength();
        }

        long next()
        {
            try
            {
                if ( position < defrag.length )
                {
                    return defrag[position];
                }
                else
                {
                    int offset = position - defrag.length;
                    return (offset < length) ? (start + offset) : VALUE_REPRESENTING_NULL;
                }
            }
            finally
            {
                ++position;
            }
        }
    }

    private static IdRangeIterator EMPTY_ID_RANGE_ITERATOR =
            new IdRangeIterator( new IdRange( new long[0], 0, 0 ) )
            {
                @Override
                long next()
                {
                    return VALUE_REPRESENTING_NULL;
                }

                ;
            };
}
