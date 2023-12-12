/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import static java.lang.Long.max;
import static java.lang.Long.min;
import static org.neo4j.collection.PrimitiveLongCollections.range;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.collection.PrimitiveLongCollections.RangedLongIterator;
import org.neo4j.internal.helpers.progress.ProgressListener;

/**
 * Returns ids either backwards or forwards. In both directions ids are returned batch-wise, sequentially forwards
 * in each batch. This means for example that in a range of ]100-0] (i.e. from 100 (exclusive) to 0 (inclusive)
 * going backwards with a batch size of 40 then ids are returned like this: 80-99, 40-79, 0-39.
 * This to get higher mechanical sympathy.
 */
public interface RecordIdIterator {
    /**
     * @return next batch of ids as {@link LongIterator}, or {@code null} if there are no more ids to return.
     */
    RangedLongIterator nextBatch();

    static RecordIdIterator backwards(long lowIncluded, long highExcluded, Configuration config) {
        return new Backwards(lowIncluded, highExcluded, config);
    }

    static RecordIdIterator forwards(long lowIncluded, long highExcluded, Configuration config) {
        return new Forwards(lowIncluded, highExcluded, config);
    }

    class Forwards implements RecordIdIterator {
        private final long lowIncluded;
        private final long highExcluded;
        private final int batchSize;
        private long startId;

        public Forwards(long lowIncluded, long highExcluded, Configuration config) {
            this.lowIncluded = lowIncluded;
            this.highExcluded = highExcluded;
            this.batchSize = config.batchSize();
            this.startId = lowIncluded;
        }

        @Override
        public RangedLongIterator nextBatch() {
            if (startId >= highExcluded) {
                return null;
            }

            long endId = min(highExcluded, findRoofId(startId));
            var result = range(startId, endId - 1 /*excluded*/);
            startId = endId;
            return result;
        }

        private long findRoofId(long floorId) {
            int rest = (int) (floorId % batchSize);
            return max(rest == 0 ? floorId + batchSize : floorId + batchSize - rest, lowIncluded);
        }

        @Override
        public String toString() {
            return "[" + lowIncluded + "-" + highExcluded + "[";
        }
    }

    class Backwards implements RecordIdIterator {
        private final long lowIncluded;
        private final long highExcluded;
        private final int batchSize;
        private long endId;

        public Backwards(long lowIncluded, long highExcluded, Configuration config) {
            this.lowIncluded = lowIncluded;
            this.highExcluded = highExcluded;
            this.batchSize = config.batchSize();
            this.endId = highExcluded;
        }

        @Override
        public RangedLongIterator nextBatch() {
            if (endId <= lowIncluded) {
                return null;
            }

            long startId = findFloorId(endId);
            var result = range(startId, endId - 1 /*excluded*/);
            endId = max(lowIncluded, startId);
            return result;
        }

        private long findFloorId(long roofId) {
            int rest = (int) (roofId % batchSize);
            return max(rest == 0 ? roofId - batchSize : roofId - rest, lowIncluded);
        }

        @Override
        public String toString() {
            return "]" + highExcluded + "-" + lowIncluded + "]";
        }
    }

    static RecordIdIterator withProgress(RecordIdIterator iterator, ProgressListener progressListener) {
        return () -> {
            var actual = iterator.nextBatch();
            if (actual == null) {
                return null;
            }
            return new RangedLongIterator() {
                @Override
                public long startInclusive() {
                    return actual.startInclusive();
                }

                @Override
                public long endExclusive() {
                    return actual.endExclusive();
                }

                @Override
                public long next() {
                    progressListener.add(1);
                    return actual.next();
                }

                @Override
                public boolean hasNext() {
                    return actual.hasNext();
                }
            };
        };
    }
}
