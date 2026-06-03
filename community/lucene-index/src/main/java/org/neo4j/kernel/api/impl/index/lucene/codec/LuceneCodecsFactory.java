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
package org.neo4j.kernel.api.impl.index.lucene.codec;

import java.util.concurrent.ExecutorService;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;

public interface LuceneCodecsFactory {
    /**
     * Returns a codec for reading or writing vector index segments. The merge path is single-threaded.
     */
    default LuceneCodec codecFor(VectorIndexConfig config) {
        return codecFor(config, 1, null);
    }

    /**
     * Returns a codec for writing vector index segments with optional intra-merge parallelism.
     *
     * @param numMergeWorkers number of worker threads to parallelize each HNSW graph merge across.
     *                        Must be {@code >= 1}. If {@code 1}, {@code mergeExec} must be {@code null}.
     * @param mergeExec executor service backing intra-merge parallelism, or {@code null} for serial merging.
     */
    LuceneCodec codecFor(VectorIndexConfig config, int numMergeWorkers, ExecutorService mergeExec);
}
