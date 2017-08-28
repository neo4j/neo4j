/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.id;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.IdContainer;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdRangeIterator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Math.max;

import static org.neo4j.kernel.impl.store.id.IdRangeIterator.EMPTY_ID_RANGE_ITERATOR;
import static org.neo4j.kernel.impl.store.id.IdRangeIterator.VALUE_REPRESENTING_NULL;

class ReplicatedIdGenerator implements IdGenerator
{
    private final IdType idType;
    private final Log log;
    private final ReplicatedIdRangeAcquirer acquirer;
    private volatile long highId;
    private volatile IdRangeIterator idQueue = EMPTY_ID_RANGE_ITERATOR;
    private final IdContainer idContainer;
    private final ReentrantLock idContainerLock = new ReentrantLock();

    ReplicatedIdGenerator( FileSystemAbstraction fs, File file, IdType idType, long highId,
            ReplicatedIdRangeAcquirer acquirer, LogProvider logProvider, int grabSize, boolean aggressiveReuse )
    {
        this.idType = idType;
        this.highId = highId;
        this.acquirer = acquirer;
        this.log = logProvider.getLog( getClass() );
        idContainer = new IdContainer( fs, file, grabSize, aggressiveReuse );
        idContainer.init();
    }

    @Override
    public void close()
    {
        idContainerLock.lock();
        try
        {
            idContainer.close( highId );
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    @Override
    public void freeId( long id )
    {
        idContainerLock.lock();
        try
        {
            idContainer.freeId( id );
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    @Override
    public long getHighId()
    {
        return highId;
    }

    @Override
    public void setHighId( long id )
    {
        this.highId = max( this.highId, id );
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        return highId - 1;
    }

    @Override
    public long getNumberOfIdsInUse()
    {
        return highId - getDefragCount();
    }

    @Override
    public synchronized long nextId()
    {
        long id = getReusableId();
        if ( id != IdContainer.NO_RESULT )
        {
            return id;
        }

        long nextId = idQueue.nextId();
        if ( nextId == VALUE_REPRESENTING_NULL )
        {
            acquireNextIdBatch();
            nextId = idQueue.nextId();
        }
        highId = max( highId, nextId + 1 );
        return nextId;
    }

    private void acquireNextIdBatch()
    {
        IdAllocation allocation = acquirer.acquireIds( idType );

        assert allocation.getIdRange().getRangeLength() > 0;
        log.debug( "Received id allocation " + allocation + " for " + idType );
        storeLocally( allocation );
    }

    @Override
    public synchronized IdRange nextIdBatch( int size )
    {
        IdRange idBatch = getReusableIdBatch( size );
        if ( idBatch.totalSize() > 0 )
        {
            return idBatch;
        }
        IdRange range = idQueue.nextIdBatch( size );
        if ( range.totalSize() == 0 )
        {
            acquireNextIdBatch();
            range = idQueue.nextIdBatch( size );
        }
        return range;
    }

    @Override
    public long getDefragCount()
    {
        idContainerLock.lock();
        try
        {
            return idContainer.getFreeIdCount();
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    @Override
    public void delete()
    {
        idContainerLock.lock();
        try
        {
            idContainer.delete();
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + this.idQueue + "]";
    }

    static void createGenerator( FileSystemAbstraction fs, File fileName, long highId,
            boolean throwIfFileExists )
    {
        IdContainer.createEmptyIdFile( fs, fileName, highId, throwIfFileExists );
    }

    private long getReusableId()
    {
        idContainerLock.lock();
        try
        {
            return idContainer.getReusableId();
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    private IdRange getReusableIdBatch( int maxSize )
    {
        idContainerLock.lock();
        try
        {
            return idContainer.getReusableIdBatch( maxSize );
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    private void storeLocally( IdAllocation allocation )
    {
        setHighId( allocation.getHighestIdInUse() + 1 ); // high id is certainly bigger than the highest id in use
        this.idQueue = respectingHighId( allocation.getIdRange() ).iterator();
    }

    private IdRange respectingHighId( IdRange idRange )
    {
        int adjustment = 0;
        long originalRangeStart = idRange.getRangeStart();
        if ( highId > originalRangeStart )
        {
            adjustment = (int) (highId - originalRangeStart);
        }
        long rangeStart = max( this.highId, originalRangeStart );
        int rangeLength = idRange.getRangeLength() - adjustment;
        if ( rangeLength <= 0 )
        {
            throw new IllegalStateException(
                    "IdAllocation state is probably corrupted or out of sync with the cluster. " +
                            "Local highId is " + highId + " and allocation range is " + idRange );
        }
        return new IdRange( idRange.getDefragIds(), rangeStart, rangeLength );
    }
}
