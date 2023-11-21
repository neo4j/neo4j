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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.NonUniqueIndexSampler;

/**
 * {@link NonUniqueIndexSampler} which performs a full scans of a {@link GBPTree} in {@link NonUniqueIndexSampler#sample(CursorContext, AtomicBoolean)}.
 *
 * @param <KEY> type of keys in tree.
 */
class FullScanNonUniqueIndexSampler<KEY extends NativeIndexKey<KEY>> extends NonUniqueIndexSampler.Adapter {
    private final GBPTree<KEY, NullValue> gbpTree;
    private final IndexLayout<KEY> layout;

    FullScanNonUniqueIndexSampler(GBPTree<KEY, NullValue> gbpTree, IndexLayout<KEY> layout) {
        this.gbpTree = gbpTree;
        this.layout = layout;
    }

    @Override
    public IndexSample sample(CursorContext cursorContext, AtomicBoolean stopped) {
        KEY lowest = layout.newKey();
        lowest.initialize(Long.MIN_VALUE);
        lowest.initValuesAsLowest();
        KEY highest = layout.newKey();
        highest.initialize(Long.MAX_VALUE);
        highest.initValuesAsHighest();
        KEY prev = layout.newKey();
        try (Seeker<KEY, NullValue> seek = gbpTree.seek(lowest, highest, cursorContext)) {
            long sampledValues = 0;
            long uniqueValues = 0;

            // Get the first one so that prev gets initialized
            if (seek.next()) {
                prev = layout.copyKey(seek.key(), prev);
                sampledValues++;
                uniqueValues++;

                // Then do the rest
                while (seek.next()) {
                    if (stopped.get()) {
                        return new IndexSample();
                    }

                    if (layout.compareValue(prev, seek.key()) != 0) {
                        uniqueValues++;
                        layout.copyKey(seek.key(), prev);
                    }
                    // else this is a duplicate of the previous one
                    sampledValues++;
                }
            }
            return new IndexSample(sampledValues, uniqueValues, sampledValues);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IndexSample sample(int numDocs, CursorContext cursorContext) {
        throw new UnsupportedOperationException();
    }
}
