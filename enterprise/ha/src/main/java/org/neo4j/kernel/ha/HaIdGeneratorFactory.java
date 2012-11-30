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

import java.io.File;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import org.neo4j.com.Response;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdRange;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

public class HaIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<IdType, HaIdGenerator> generators =
            new EnumMap<IdType, HaIdGenerator>( IdType.class );
    private final IdGeneratorFactory localFactory = new DefaultIdGeneratorFactory();
    private final Master master;
    private final StringLogger logger;
    private IdGeneratorState globalState = IdGeneratorState.PENDING;

    public HaIdGeneratorFactory( Master master, HighAvailability highAvailability, Logging logging )
    {
        this.master = master;
        this.logger = logging.getLogger( getClass() );
//        highAvailability.addHighAvailabilityMemberListener( new HaIdGeneratorFactoryClusterMemberListener() );
    }

    @Override
    public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType, long highId )
    {
        HaIdGenerator previous = generators.remove( idType );
        if ( previous != null )
            previous.close();
        
        IdGenerator initialIdGenerator = null;
        switch ( globalState )
        {
        case MASTER:
            initialIdGenerator = localFactory.open( fs, fileName, grabSize, idType, highId );
            break;
        case SLAVE:
            initialIdGenerator = new SlaveIdGenerator( idType, highId, master, logger );
            break;
        default:
            throw new IllegalStateException( globalState.name() );
        }
        HaIdGenerator haIdGenerator = new HaIdGenerator( initialIdGenerator, fs, fileName, grabSize, idType, globalState );
        generators.put( idType, haIdGenerator );
        return haIdGenerator;
    }

    @Override
    public void create( FileSystemAbstraction fs, File fileName, long highId )
    {
        localFactory.create( fs, fileName, highId );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return generators.get( idType );
    }
    
//    private class HaIdGeneratorFactoryClusterMemberListener implements HighAvailabilityMemberListener
//    {
//        @Override
//        public void masterIsElected( HighAvailabilityMemberChangeEvent event )
//        {
//            if ( event.getNewState() == event.getOldState() )
//            {
//                return;
//            }
//            for ( HaIdGenerator generator : generators.values() )
//            {
//                generator.stateChanged( event );
//            }
//        }
//
//        @Override
//        public void masterIsAvailable( HighAvailabilityMemberChangeEvent event )
//        {
//            if ( event.getNewState() == event.getOldState() )
//            {
//                return;
//            }
//            for ( HaIdGenerator generator : generators.values() )
//            {
//                generator.stateChanged( event );
//            }
//        }
//
//        @Override
//        public void slaveIsAvailable( HighAvailabilityMemberChangeEvent event )
//        {
//            if ( event.getNewState() == event.getOldState() )
//            {
//                return;
//            }
//            for ( HaIdGenerator generator : generators.values() )
//            {
//                generator.stateChanged( event );
//            }
//        }
//
//        @Override
//        public void instanceStops( HighAvailabilityMemberChangeEvent event )
//        {
//            if ( event.getNewState() == event.getOldState() )
//            {
//                return;
//            }
//            for ( HaIdGenerator generator : generators.values() )
//            {
//                generator.stateChanged( event );
//            }
//        }
//    }
    
    public void switchToMaster()
    {
        globalState = IdGeneratorState.MASTER;
        for ( HaIdGenerator generator : generators.values() )
        {
            generator.switchToMaster();
        }
    }
    
    public void switchToSlave()
    {
        globalState = IdGeneratorState.SLAVE;
        for ( HaIdGenerator generator : generators.values() )
        {
            generator.switchToSlave();
        }
    }

    private static final long VALUE_REPRESENTING_NULL = -1;

    private enum IdGeneratorState
    {
        PENDING, SLAVE, MASTER;
    }

    private class HaIdGenerator implements IdGenerator
    {
        private IdGenerator delegate;
        private final FileSystemAbstraction fs;
        private final File fileName;
        private final int grabSize;
        private final IdType idType;
        private volatile IdGeneratorState state;

        HaIdGenerator( IdGenerator initialDelegate, FileSystemAbstraction fs, File fileName, int grabSize,
                       IdType idType, IdGeneratorState initialState )
        {
            delegate = initialDelegate;
            this.fs = fs;
            this.fileName = fileName;
            this.grabSize = grabSize;
            this.idType = idType;
            this.state = initialState;
            logger.debug( "New " + this + ", " + initialDelegate + " " + idType + ", " + initialState );
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
//        public void stateChanged( HighAvailabilityMemberChangeEvent event )
//        {
//            
//            // Assume blockade is up and no active threads are running here
//            if ( event.getNewState() == HighAvailabilityMemberState.TO_MASTER ||
//                    // When the first PENDING --> TO_MASTER event comes there are no HaIdGenerator
//                    // instances available because that event reaches HighAvailabilityModeSwitcher
//                    // first, which starts the data sources and in turn instantiates these id generators.
//                    event.getNewState() == HighAvailabilityMemberState.MASTER )
//            {
//                switchToMaster();
//            }
//            else if ( event.getNewState() == HighAvailabilityMemberState.TO_SLAVE
//                    // When the first PENDING --> TO_SLAVE event comes there are no HaIdGenerator
//                    // instances available because that event reaches HighAvailabilityModeSwitcher
//                    // first, which starts the data sources and in turn instantiates these id generators.
//                    || event.getNewState() == HighAvailabilityMemberState.SLAVE )
//            {
//                switchToSlave();
//            }
//        }

        private void switchToSlave()
        {
            long highId = delegate.getHighId();
            delegate.close();
            delegate = new SlaveIdGenerator( idType, highId, master, logger );
            logger.debug( "Instantiated " + delegate + " with highid " + highId );
            state = IdGeneratorState.SLAVE;
        }

        private void switchToMaster()
        {
            if ( state == IdGeneratorState.SLAVE )
            {
                long highId = delegate.getHighId();
                delegate.close();
                if ( fs.fileExists( fileName ) )
                    fs.deleteFile( fileName );
                    
                localFactory.create( fs, fileName, highId );
                delegate = localFactory.open( fs, fileName, grabSize, idType, highId );
                logger.debug( "Instantiated " + delegate + " of type " + idType + " with highid " + highId );
            }
            else
                logger.debug( "Keeps " + delegate );
            // Otherwise we're master or TBD (initial state) which is the same
            state = IdGeneratorState.MASTER;
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
            if ( state == IdGeneratorState.PENDING )
                throw new IllegalStateException( state.name() );
            
            long result = delegate.nextId();
            logger.debug( "Using id " + result + " from " + delegate + " of type " + idType );
            return result;
        }

        public IdRange nextIdBatch( int size )
        {
            if ( state == IdGeneratorState.PENDING )
                throw new IllegalStateException( state.name() );
            
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
        private final Master master;
        private final IdType idType;
        private final StringLogger logger;

        SlaveIdGenerator( IdType idType, long highId, Master master, StringLogger logger )
        {
            this.idType = idType;
            this.highestIdInUse = highId;
            this.master = master;
            this.logger = logger;
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
                try
                {
                    IdAllocation allocation = response.response();
                    logger.info( "Received id allocation " + allocation + " from master " + master + " for " + idType );
                    nextId = storeLocally( allocation );
                }
                finally
                {
                    response.close();
                }
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
        
        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[" + this.idQueue + "]";
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
        
        @Override
        public String toString()
        {
            return "IdRangeIterator[start:" + start + ", length:" + length + ", position:" + position + ", defrag:" + Arrays.toString( defrag ) + "]";
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
