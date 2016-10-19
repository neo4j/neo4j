/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Math.max;
import static org.neo4j.causalclustering.core.state.machines.id.IdRangeIterator.EMPTY_ID_RANGE_ITERATOR;
import static org.neo4j.causalclustering.core.state.machines.id.IdRangeIterator.VALUE_REPRESENTING_NULL;

class ReplicatedIdGenerator implements IdGenerator
{
    private final IdType idType;
    private final Log log;
    private final ReplicatedIdRangeAcquirer acquirer;
    private volatile long highId;
    private volatile long defragCount;
    private volatile IdRangeIterator idQueue = EMPTY_ID_RANGE_ITERATOR;

    ReplicatedIdGenerator( IdType idType, long highId, ReplicatedIdRangeAcquirer acquirer, LogProvider
            logProvider )
    {
        this.idType = idType;
        this.highId = highId;
        this.acquirer = acquirer;
        this.log = logProvider.getLog( getClass() );
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
        return highId - defragCount;
    }

    @Override
    public synchronized long nextId()
    {
        long nextId = idQueue.next();
        if ( nextId == VALUE_REPRESENTING_NULL )
        {
            IdAllocation allocation;
            allocation = acquirer.acquireIds( idType );

            assert allocation.getIdRange().getRangeLength() > 0;
            log.debug( "Received id allocation " + allocation + " for " + idType );
            nextId = storeLocally( allocation );
        }
        highId = max( highId, nextId + 1 );
        return nextId;
    }

    @Override
    public IdRange nextIdBatch( int size )
    {
        throw new UnsupportedOperationException( "Should never be called" );
    }

    private long storeLocally( IdAllocation allocation )
    {
        setHighId( allocation.getHighestIdInUse() + 1 ); // high id is certainly bigger than the highest id in use
        this.defragCount = allocation.getDefragCount();
        this.idQueue = new IdRangeIterator( respectingHighId( allocation.getIdRange() ) );
        return idQueue.next();
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
