/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.store.counts;

import java.io.IOException;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.EntryUpdater;
import org.neo4j.kernel.impl.store.kvstore.ValueUpdate;
import org.neo4j.kernel.impl.store.kvstore.WritableBuffer;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexStatisticsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;

final class CountsUpdater implements CountsAccessor.Updater, CountsAccessor.IndexStatsUpdater, AutoCloseable
{
    private final EntryUpdater<CountsKey> updater;

    CountsUpdater( EntryUpdater<CountsKey> updater )
    {
        this.updater = updater;
    }

    /**
     * Value format:
     * <pre>
     *  0 1 2 3 4 5 6 7   8 9 A B C D E F
     * [0,0,0,0,0,0,0,0 ; c,c,c,c,c,c,c,c]
     *  c - number of matching nodes
     * </pre>
     * For key format, see {@link KeyFormat#visitNodeCount(int, long)}
     */
    @Override
    public void incrementNodeCount( long labelId, long delta )
    {
        try
        {
            updater.apply( nodeKey( labelId ), incrementSecondBy( delta ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Value format:
     * <pre>
     *  0 1 2 3 4 5 6 7   8 9 A B C D E F
     * [0,0,0,0,0,0,0,0 ; c,c,c,c,c,c,c,c]
     *  c - number of matching relationships
     * </pre>
     * For key format, see {@link KeyFormat#visitRelationshipCount(int, int, int, long)}
     */
    @Override
    public void incrementRelationshipCount( long startLabelId, int typeId, long endLabelId, long delta )
    {
        try
        {
            updater.apply( relationshipKey( startLabelId, typeId, endLabelId ), incrementSecondBy( delta ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Value format:
     * <pre>
     *  0 1 2 3 4 5 6 7   8 9 A B C D E F
     * [u,u,u,u,u,u,u,u ; s,s,s,s,s,s,s,s]
     *  u - number of updates
     *  s - size of index
     * </pre>
     * For key format, see {@link KeyFormat#visitIndexStatistics(long, long, long)}
     */
    @Override
    public void replaceIndexUpdateAndSize( long indexId, long updates, long size )
    {
        try
        {
            updater.apply( indexStatisticsKey( indexId ), new Write( updates, size ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Value format:
     * <pre>
     *  0 1 2 3 4 5 6 7   8 9 A B C D E F
     * [u,u,u,u,u,u,u,u ; s,s,s,s,s,s,s,s]
     *  u - number of unique values
     *  s - size of index
     * </pre>
     * For key format, see {@link KeyFormat#visitIndexSample(long, long, long)}
     */
    @Override
    public void replaceIndexSample( long indexId, long unique, long size )
    {
        try
        {
            updater.apply( indexSampleKey( indexId ), new Write( unique, size ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * For key format, see {@link KeyFormat#visitIndexStatistics(long, long, long)}
     * For value format, see {@link CountsUpdater#replaceIndexUpdateAndSize(long, long, long)}
     */
    @Override
    public void incrementIndexUpdates( long indexId, long delta )
    {
        try
        {
            updater.apply( indexStatisticsKey( indexId ), incrementFirstBy( delta ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public void close()
    {
        updater.close();
    }

    private static class Write implements ValueUpdate
    {
        private final long first;
        private final long second;

        Write( long first, long second )
        {
            this.first = first;
            this.second = second;
        }

        @Override
        public void update( WritableBuffer target )
        {
            target.putLong( 0, first );
            target.putLong( 8, second );
        }
    }

    private static IncrementLong incrementFirstBy( long delta )
    {
        return new IncrementLong( 0, delta );
    }

    private static IncrementLong incrementSecondBy( long delta )
    {
        return new IncrementLong( 8, delta );
    }

    private static class IncrementLong implements ValueUpdate
    {
        private final int offset;
        private final long delta;

        private IncrementLong( int offset, long delta )
        {
            this.offset = offset;
            this.delta = delta;
        }

        @Override
        public void update( WritableBuffer target )
        {
            target.putLong( offset, target.getLong( offset ) + delta );
        }
    }
}
