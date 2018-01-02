/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.id;

import java.io.File;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.IdTypeConfiguration;
import org.neo4j.kernel.IdTypeConfigurationProvider;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

public class HaIdGeneratorFactory implements IdGeneratorFactory
{
    private final Map<IdType,HaIdGenerator> generators = new EnumMap<>( IdType.class );
    private final FileSystemAbstraction fs;
    private IdTypeConfigurationProvider idTypeConfigurationProvider;
    private final IdGeneratorFactory localFactory;
    private final DelegateInvocationHandler<Master> master;
    private final Log log;
    private final RequestContextFactory requestContextFactory;
    private IdGeneratorState globalState = IdGeneratorState.PENDING;

    public HaIdGeneratorFactory( DelegateInvocationHandler<Master> master, LogProvider logProvider,
            RequestContextFactory requestContextFactory, FileSystemAbstraction fs, IdTypeConfigurationProvider idTypeConfigurationProvider )
    {
        this.fs = fs;
        this.idTypeConfigurationProvider = idTypeConfigurationProvider;
        this.localFactory = new DefaultIdGeneratorFactory( fs, idTypeConfigurationProvider );
        this.master = master;
        this.log = logProvider.getLog( getClass() );
        this.requestContextFactory = requestContextFactory;
    }

    @Override
    public IdGenerator open( File filename, IdType idType, long highId )
    {
        IdTypeConfiguration idTypeConfiguration = idTypeConfigurationProvider.getIdTypeConfiguration( idType );
        return open( filename, idTypeConfiguration.getGrabSize(), idType, highId );
    }

    @Override
    public IdGenerator open( File fileName, int grabSize, IdType idType, long highId )
    {
        HaIdGenerator previous = generators.remove( idType );
        if ( previous != null )
        {
            previous.close();
        }

        IdGenerator initialIdGenerator;
        switch ( globalState )
        {
        case MASTER:
            initialIdGenerator = localFactory.open( fileName, grabSize, idType, highId );
            break;
        case SLAVE:
            // Initially we may call switchToSlave() before calling open, so we need this additional
            // (and, you might say, hacky) call to delete the .id file here as well as in switchToSlave().
            fs.deleteFile( fileName );
            initialIdGenerator = new SlaveIdGenerator( idType, highId, master.cement(), log, requestContextFactory );
            break;
        default:
            throw new IllegalStateException( globalState.name() );
        }
        HaIdGenerator haIdGenerator = new HaIdGenerator( initialIdGenerator, fs, fileName, grabSize, idType, globalState );
        generators.put( idType, haIdGenerator );
        return haIdGenerator;
    }

    @Override
    public void create( File fileName, long highId, boolean throwIfFileExists )
    {
        localFactory.create( fileName, highId, false );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return generators.get( idType );
    }

    public void switchToMaster()
    {
        globalState = IdGeneratorState.MASTER;
        for ( HaIdGenerator generator : generators.values() )
        {
            generator.switchToMaster();
        }
    }

    public void enableCompatibilityMode()
    {
        /*
         * When a slave is running in compatibility mode (i.e. with a master which is of the previous version), it may
         * so be that it requires access to some id generator that the master is unaware of. Prominent example is
         * Relationship Group id generator existing in 2.1 but not in 2.0. When this happens, the slave must rely
         * on the local id generator and not expect delegations from the master. SwitchToSlave should detect when
         * this is necessary and properly flip the appropriate generators to "local mode", i.e. master mode.
         */

        generators.get( IdType.RELATIONSHIP_GROUP ).switchToMaster();
    }

    public void switchToSlave()
    {
        globalState = IdGeneratorState.SLAVE;
        for ( HaIdGenerator generator : generators.values() )
        {
            generator.switchToSlave( master.cement() );
        }
    }

    static final long VALUE_REPRESENTING_NULL = -1;

    private enum IdGeneratorState
    {
        PENDING, SLAVE, MASTER;
    }

    private class HaIdGenerator implements IdGenerator
    {
        private volatile IdGenerator delegate;
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
            log.debug( "Instantiated HaIdGenerator for " + initialDelegate + " " + idType + ", " + initialState );
        }

        private void switchToSlave( Master master )
        {
            long highId = delegate.getHighId();
            // The .id file is open and marked DIRTY
            delegate.delete();
            // The .id file underneath is now gone
            delegate = new SlaveIdGenerator( idType, highId, master, log, requestContextFactory );
            log.debug( "Instantiated slave delegate " + delegate + " of type " + idType + " with highid " + highId );
            state = IdGeneratorState.SLAVE;
        }

        private void switchToMaster()
        {
            if ( state == IdGeneratorState.SLAVE )
            {
                long highId = delegate.getHighId();
                delegate.delete();

                localFactory.create( fileName, highId, false );
                delegate = localFactory.open( fileName, grabSize, idType, highId );
                log.debug( "Instantiated master delegate " + delegate + " of type " + idType + " with highid " + highId );
            }
            else
            {
                log.debug( "Keeps " + delegate );
            }

            state = IdGeneratorState.MASTER;
        }

        @Override
        public String toString()
        {
            return delegate.toString();
        }

        @Override
        public final boolean equals( Object other )
        {
            return delegate.equals( other );
        }

        @Override
        public final int hashCode()
        {
            return delegate.hashCode();
        }

        @Override
        public long nextId()
        {
            if ( state == IdGeneratorState.PENDING )
            {
                throw new IllegalStateException( state.name() );
            }

            long result = delegate.nextId();
            return result;
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            if ( state == IdGeneratorState.PENDING )
            {
                throw new IllegalStateException( state.name() );
            }

            return delegate.nextIdBatch( size );
        }

        @Override
        public void setHighId( long id )
        {
            delegate.setHighId( id );
        }

        @Override
        public long getHighId()
        {
            return delegate.getHighId();
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return delegate.getHighestPossibleIdInUse();
        }

        @Override
        public void freeId( long id )
        {
            delegate.freeId( id );
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return delegate.getNumberOfIdsInUse();
        }

        @Override
        public long getDefragCount()
        {
            return delegate.getDefragCount();
        }

        @Override
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
        private final Log log;
        private final RequestContextFactory requestContextFactory;

        SlaveIdGenerator( IdType idType, long highId, Master master, Log log,
                RequestContextFactory requestContextFactory )
        {
            this.idType = idType;
            this.highestIdInUse = highId;
            this.master = master;
            this.log = log;
            this.requestContextFactory = requestContextFactory;
        }

        @Override
        public void close()
        {
        }

        @Override
        public void freeId( long id )
        {
        }

        @Override
        public long getHighId()
        {
            return highestIdInUse;
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return highestIdInUse;
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return highestIdInUse - defragCount;
        }

        @Override
        public synchronized long nextId()
        {
            long nextId = nextLocalId();
            if ( nextId == VALUE_REPRESENTING_NULL )
            {
                // If we don't have anymore grabbed ids from master, grab a bunch
                try ( Response<IdAllocation> response =
                        master.allocateIds( requestContextFactory.newRequestContext(), idType ) )
                {
                    IdAllocation allocation = response.response();
                    log.info( "Received id allocation " + allocation + " from master " + master + " for " + idType );
                    nextId = storeLocally( allocation );
                }
                catch ( ComException e )
                {
                    throw new TransientTransactionFailureException(
                            "Cannot allocate new entity ids from the cluster master. " +
                            "The master instance is either down, or we have network connectivity problems", e );
                }
            }
            return nextId;
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            throw new UnsupportedOperationException( "Should never be called" );
        }

        private long storeLocally( IdAllocation allocation )
        {
            setHighId( allocation.getHighestIdInUse() );
            this.defragCount = allocation.getDefragCount();
            this.idQueue = new IdRangeIterator( allocation.getIdRange() );
            return idQueue.next();
        }

        private long nextLocalId()
        {
            return this.idQueue.next();
        }

        @Override
        public void setHighId( long id )
        {
            this.highestIdInUse = Math.max( this.highestIdInUse, id );
        }

        @Override
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

    static class IdRangeIterator
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

                long candidate = nextRangeCandidate();
                if ( candidate == IdGeneratorImpl.INTEGER_MINUS_ONE )
                {
                    position++;
                    candidate = nextRangeCandidate();
                }
                return candidate;
            }
            finally
            {
                ++position;
            }
        }

        private long nextRangeCandidate()
        {
            int offset = position - defrag.length;
            return (offset < length) ? (start + offset) : VALUE_REPRESENTING_NULL;
        }

        @Override
        public String toString()
        {
            return "IdRangeIterator[start:" + start + ", length:" + length + ", position:" + position + ", defrag:" + Arrays.toString( defrag ) + "]";
        }
    }

    private static IdRangeIterator EMPTY_ID_RANGE_ITERATOR =
            new IdRangeIterator( new IdRange( EMPTY_LONG_ARRAY, 0, 0 ) )
            {
                @Override
                long next()
                {
                    return VALUE_REPRESENTING_NULL;
                }
            };
}
