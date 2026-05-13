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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;

public class Neo4j202605BinaryVectorCodec extends FilterCodec implements Lucene10Codec {
    private static final String CODEC_NAME = "Neo4j202605BinaryVectorCodec";
    private final KnnVectorsFormat vectorFormat;

    /// Used by Lucene Service Loader when reading segments, will load actual values from disk
    public Neo4j202605BinaryVectorCodec() {
        this(VectorIndexConfig.EMPTY);
    }

    /// Used for writing and created programmatically when creating the IndexWriter
    public Neo4j202605BinaryVectorCodec(VectorIndexConfig config) {
        super(CODEC_NAME, new Lucene104Codec());
        int maxDimensions = config.maxDimensions();
        this.vectorFormat = new LuceneKnnBinaryQuantizedVectorFormat(maxDimensions, config.hnsw());
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
