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
package org.neo4j.kernel.api.impl.schema.vector.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;

public class VectorCodecV2 extends Lucene99Codec {
    private final KnnVectorsFormat vectorFormat;

    public VectorCodecV2(VectorIndexConfig config) {
        super();
        final var dimensions =
                config.dimensions().orElseGet(() -> config.version().maxDimensions());
        if (config.quantizationEnabled()) {
            this.vectorFormat = new LuceneKnnScalarQuantizedVectorFormatV2(dimensions, config.hnsw());
        } else {
            this.vectorFormat = new LuceneKnnVectorFormatV2(dimensions, config.hnsw());
        }
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return vectorFormat;
    }
}
