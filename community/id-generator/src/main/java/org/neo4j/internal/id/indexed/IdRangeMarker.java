/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.id.indexed;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.id.IdGenerator.CommitMarker;
import org.neo4j.internal.id.IdGenerator.ReuseMarker;

import static java.lang.Math.toIntExact;
import static org.neo4j.internal.id.IdValidator.isReservedId;

/**
 * Contains logic for merging ID state changes into the tree backing an {@link IndexedIdGenerator}.
 * Basically manipulates {@link IdRangeKey} and {@link IdRange} instances and sends to {@link Writer#merge(Object, Object, ValueMerger)}.
 */
class IdRangeMarker implements CommitMarker, ReuseMarker
{
    private final int idsPerEntry;
    private final Writer<IdRangeKey, IdRange> writer;
    private final Lock lock;
    private final ValueMerger<IdRangeKey, IdRange> merger;
    private final boolean started;
    private final AtomicBoolean freeIdsNotifier;
    private final long generation;
    private final AtomicLong highestWrittenId;
    private final boolean bridgeIdGaps;
    private final IdRangeKey key;
    private final IdRange value;

    IdRangeMarker( int idsPerEntry, Layout<IdRangeKey,IdRange> layout, Writer<IdRangeKey,IdRange> writer, Lock lock, ValueMerger<IdRangeKey,IdRange> merger,
            boolean started, AtomicBoolean freeIdsNotifier, long generation, AtomicLong highestWrittenId, boolean bridgeIdGaps )
    {
        this.idsPerEntry = idsPerEntry;
        this.writer = writer;
        this.key = layout.newKey();
        this.value = layout.newValue();
        this.lock = lock;
        this.merger = merger;
        this.started = started;
        this.freeIdsNotifier = freeIdsNotifier;
        this.generation = generation;
        this.highestWrittenId = highestWrittenId;
        this.bridgeIdGaps = bridgeIdGaps;
    }

    @Override
    public void close()
    {
        try
        {
            writer.close();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public void markUsed( long id )
    {
        bridgeGapBetweenHighestWrittenIdAndThisId( id );
        if ( !isReservedId( id ) )
        {
            // this is by convention: if reserved ID is marked as RESERVED again then it becomes USED
            prepareRange( id, false );
            value.setCommitAndReuseBit( idOffset( id ) );
            writer.mergeIfExists( key, value, merger );
        }
    }

    @Override
    public void markDeleted( long id )
    {
        bridgeGapBetweenHighestWrittenIdAndThisId( id );
        if ( !isReservedId( id ) )
        {
            prepareRange( id, true );
            value.setCommitBit( idOffset( id ) );
            writer.merge( key, value, merger );
        }
    }

    @Override
    public void markReserved( long id )
    {
        if ( !isReservedId( id ) )
        {
            prepareRange( id, false );
            value.setReuseBit( idOffset( id ) );
            writer.merge( key, value, merger );
        }
    }

    @Override
    public void markFree( long id )
    {
        bridgeGapBetweenHighestWrittenIdAndThisId( id );
        if ( !isReservedId( id ) )
        {
            prepareRange( id, true );
            value.setReuseBit( idOffset( id ) );
            writer.merge( key, value, merger );
        }

        freeIdsNotifier.set( true );
    }

    private void prepareRange( long id, boolean addition )
    {
        key.setIdRangeIdx( id / idsPerEntry );
        value.clear( generation, addition );
    }

    private int idOffset( long id )
    {
        return toIntExact( id % idsPerEntry );
    }

    private void bridgeGapBetweenHighestWrittenIdAndThisId( long id )
    {
        long highestWrittenId = this.highestWrittenId.get();
        if ( bridgeIdGaps && highestWrittenId < id )
        {
            while ( highestWrittenId < id - 1 )
            {
                long bridgeId = ++highestWrittenId;
                if ( !isReservedId( bridgeId ) )
                {
                    prepareRange( bridgeId, true );
                    value.setCommitBit( idOffset( bridgeId ) );
                    if ( !started ) // i.e. in recovery mode
                    {
                        value.setReuseBit( idOffset( bridgeId ) );
                    }
                    writer.merge( key, value, merger );
                }
            }
            // Well, we bridged the gap up and including id - 1, but we know that right after this the actual id will be written
            // so to try to isolate updates to highestWrittenId to this method we can might as well do that right here.
            this.highestWrittenId.set( id );
        }
    }
}
