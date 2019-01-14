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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static java.lang.Long.max;
import static java.lang.Long.min;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.range;

/**
 * Returns ids either backwards or forwards. In both directions ids are returned batch-wise, sequentially forwards
 * in each batch. This means for example that in a range of ]100-0] (i.e. from 100 (exclusive) to 0 (inclusive)
 * going backwards with a batch size of 40 then ids are returned like this: 80-99, 40-79, 0-39.
 * This to get higher mechanical sympathy.
 */
public interface RecordIdIterator
{
    /**
     * @return next batch of ids as {@link PrimitiveLongIterator}, or {@code null} if there are no more ids to return.
     */
    PrimitiveLongIterator nextBatch();

    static RecordIdIterator backwards( long lowIncluded, long highExcluded, Configuration config )
    {
        return new Backwards( lowIncluded, highExcluded, config );
    }

    static RecordIdIterator forwards( long lowIncluded, long highExcluded, Configuration config )
    {
        return new Forwards( lowIncluded, highExcluded, config );
    }

    static RecordIdIterator allIn( RecordStore<? extends AbstractBaseRecord> store, Configuration config )
    {
        return forwards( store.getNumberOfReservedLowIds(), store.getHighId(), config );
    }

    static RecordIdIterator allInReversed( RecordStore<? extends AbstractBaseRecord> store,
            Configuration config )
    {
        return backwards( store.getNumberOfReservedLowIds(), store.getHighId(), config );
    }

    class Forwards implements RecordIdIterator
    {
        private final long lowIncluded;
        private final long highExcluded;
        private final int batchSize;
        private long startId;

        public Forwards( long lowIncluded, long highExcluded, Configuration config )
        {
            this.lowIncluded = lowIncluded;
            this.highExcluded = highExcluded;
            this.batchSize = config.batchSize();
            this.startId = lowIncluded;
        }

        @Override
        public PrimitiveLongIterator nextBatch()
        {
            if ( startId >= highExcluded )
            {
                return null;
            }

            long endId = min( highExcluded, findRoofId( startId ) );
            PrimitiveLongIterator result = range( startId, endId - 1 /*excluded*/ );
            startId = endId;
            return result;
        }

        private long findRoofId( long floorId )
        {
            int rest = (int) (floorId % batchSize);
            return max( rest == 0 ? floorId + batchSize : floorId + batchSize - rest, lowIncluded );
        }

        @Override
        public String toString()
        {
            return "[" + lowIncluded + "-" + highExcluded + "[";
        }
    }

    class Backwards implements RecordIdIterator
    {
        private final long lowIncluded;
        private final long highExcluded;
        private final int batchSize;
        private long endId;

        public Backwards( long lowIncluded, long highExcluded, Configuration config )
        {
            this.lowIncluded = lowIncluded;
            this.highExcluded = highExcluded;
            this.batchSize = config.batchSize();
            this.endId = highExcluded;
        }

        @Override
        public PrimitiveLongIterator nextBatch()
        {
            if ( endId <= lowIncluded )
            {
                return null;
            }

            long startId = findFloorId( endId );
            PrimitiveLongIterator result = range( startId, endId - 1 /*excluded*/ );
            endId = max( lowIncluded, startId );
            return result;
        }

        private long findFloorId( long roofId )
        {
            int rest = (int) (roofId % batchSize);
            return max( rest == 0 ? roofId - batchSize : roofId - rest, lowIncluded );
        }

        @Override
        public String toString()
        {
            return "]" + highExcluded + "-" + lowIncluded + "]";
        }
    }
}
