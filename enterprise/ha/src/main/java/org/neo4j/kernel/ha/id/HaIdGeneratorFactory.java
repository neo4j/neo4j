/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.com.Response;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
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
    private final DelegateInvocationHandler<Master> master;
    private final StringLogger logger;
    private final RequestContextFactory requestContextFactory;
    private IdGeneratorState globalState = IdGeneratorState.PENDING;

    public HaIdGeneratorFactory( DelegateInvocationHandler<Master> master, Logging logging,
            RequestContextFactory requestContextFactory )
    {
        this.master = master;
        this.logger = logging.getMessagesLog( getClass() );
        this.requestContextFactory = requestContextFactory;
    }

    @Override
    public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType, long highId )
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
            initialIdGenerator = localFactory.open( fs, fileName, grabSize, idType, highId );
            break;
        case SLAVE:
            initialIdGenerator = new SlaveIdGenerator( idType, highId, master.cement(), logger, requestContextFactory );
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
            generator.switchToSlave( master.cement() );
        }
    }

    private static final long VALUE_REPRESENTING_NULL = -1;

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
            logger.debug( "Instantiated HaIdGenerator for " + initialDelegate + " " + idType + ", " + initialState );
        }

        private void switchToSlave( Master master )
        {
            long highId = delegate.getHighId();
            delegate.close();
            delegate = new SlaveIdGenerator( idType, highId, master, logger, requestContextFactory );
            logger.debug( "Instantiated slave delegate " + delegate + " of type " + idType + " with highid " + highId );
            state = IdGeneratorState.SLAVE;
        }

        private void switchToMaster()
        {
            if ( state == IdGeneratorState.SLAVE )
            {
                long highId = delegate.getHighId();
                delegate.close();
                if ( fs.fileExists( fileName ) )
                {
                    fs.deleteFile( fileName );
                }
                    
                localFactory.create( fs, fileName, highId );
                delegate = localFactory.open( fs, fileName, grabSize, idType, highId );
                logger.debug( "Instantiated master delegate " + delegate + " of type " + idType + " with highid " + highId );
            }
            else
            {
                logger.debug( "Keeps " + delegate );
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
        private final StringLogger logger;
        private final RequestContextFactory requestContextFactory;

        SlaveIdGenerator( IdType idType, long highId, Master master, StringLogger logger,
                RequestContextFactory requestContextFactory )
        {
            this.idType = idType;
            this.highestIdInUse = highId;
            this.master = master;
            this.logger = logger;
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
                    logger.info( "Received id allocation " + allocation + " from master " + master + " for " + idType );
                    nextId = storeLocally( allocation );
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
