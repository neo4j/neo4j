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

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.LongFunction;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.batchimport.api.input.InputEntityVisitor;

/**
 * A utility to be able to write an {@link InputIterator} with low effort.
 * Since {@link InputIterator} is multi-threaded in that multiple threads can call {@link #newChunk()} and each
 * call to {@link #next(InputChunk)} handing out the next chunkstate instance from the supplied {@link Iterator}.
 *
 * @param <CHUNKSTATE> type of objects handed out from the supplied {@link Iterator}.
 */
public class GeneratingInputIterator<CHUNKSTATE> implements InputIterator {
    private final LongFunction<CHUNKSTATE> states;
    private final long totalCount;
    private final int batchSize;
    private final Generator<CHUNKSTATE> generator;
    private final long startId;

    private long nextBatch;
    private long numberOfBatches;

    public GeneratingInputIterator(
            long totalCount,
            int batchSize,
            LongFunction<CHUNKSTATE> states,
            Generator<CHUNKSTATE> generator,
            long startId) {
        this.totalCount = max(totalCount, 0);
        this.batchSize = max(batchSize, 0);
        this.states = states;
        this.generator = generator;
        this.startId = startId;
        this.numberOfBatches =
                this.totalCount > 0 && this.batchSize > 0 ? 1 + (this.totalCount - 1) / this.batchSize : 0;
    }

    @Override
    public void close() {}

    @Override
    public InputChunk newChunk() {
        return new Chunk();
    }

    @Override
    public synchronized boolean next(InputChunk chunk) {
        if (numberOfBatches > 1) {
            long batch = nextBatch++;
            ((Chunk) chunk).initialize(states.apply(batch), batch, batchSize);
            numberOfBatches--;
            return true;
        } else if (numberOfBatches == 1) {
            long remaining = 1 + (totalCount - 1) % batchSize;
            long batch = nextBatch++;
            ((Chunk) chunk).initialize(states.apply(batch), batch, toIntExact(remaining));
            numberOfBatches--;
            return true;
        }
        return false;
    }

    private class Chunk implements InputChunk {
        private CHUNKSTATE state;
        private int count;
        private int itemInBatch;
        private long baseId;

        @Override
        public void close() {}

        /**
         * @param state CHUNKSTATE which is the source of data generation for this chunk.
         * @param batch zero-based id (order) of this batch.
         */
        private void initialize(CHUNKSTATE state, long batch, int count) {
            this.state = state;
            this.count = count;
            this.baseId = startId + batch * batchSize;
            this.itemInBatch = 0;
        }

        @Override
        public boolean next(InputEntityVisitor visitor) throws IOException {
            if (itemInBatch < count) {
                generator.accept(state, visitor, baseId + itemInBatch);
                visitor.endOfEntity();
                itemInBatch++;
                return true;
            }
            return false;
        }
    }

    public static final InputIterator EMPTY =
            new GeneratingInputIterator<Void>(0, 1, batch -> null, null, 0) { // here's nothing
            };

    public static final InputIterable EMPTY_ITERABLE = () -> EMPTY;

    public interface Generator<CHUNKSTATE> {
        void accept(CHUNKSTATE state, InputEntityVisitor visitor, long id);
    }
}
