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
package org.neo4j.kernel.api.impl.index.lucene.v10.codec;

import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;

public class Neo4j202604ScalarVectorCodec extends FilterCodec implements Lucene10Codec {
    private static final String CODEC_NAME = "Neo4j202604ScalarVectorCodec";
    private final KnnVectorsFormat vectorFormat;

    /// Used by Lucene Service Loader when reading segments, will load actual values from disk
    public Neo4j202604ScalarVectorCodec() {
        this(VectorIndexConfig.EMPTY);
    }

    /// Used for writing and created programmatically when creating the IndexWriter
    public Neo4j202604ScalarVectorCodec(VectorIndexConfig config) {
        this(config, 1, null);
    }

    /// Used for writing with intra-merge parallelism.
    public Neo4j202604ScalarVectorCodec(VectorIndexConfig config, int numMergeWorkers, ExecutorService mergeExec) {
        super(CODEC_NAME, new Lucene104Codec());
        int maxDimensions = config.maxDimensions();
        // Lucene throws IllegalArgumentException if numMergeWorkers==1 and mergeExec!=null.
        // Fall back to the serial constructor whenever parallelism isn't useful.
        this.vectorFormat = (mergeExec == null || numMergeWorkers <= 1)
                ? new LuceneKnnScalarQuantizedVectorFormatV2(maxDimensions, config.hnsw())
                : new LuceneKnnScalarQuantizedVectorFormatV2(maxDimensions, config.hnsw(), numMergeWorkers, mergeExec);
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return vectorFormat;
    }

    @Override
    public Codec codec() {
        return this;
    }
}
