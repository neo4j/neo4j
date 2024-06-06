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
import org.neo4j.kernel.api.vector.VectorQuantization;

public class VectorCodecV2 extends Lucene99Codec {
    private final KnnVectorsFormat vectorFormat;

    public VectorCodecV2(int maxDimensions, VectorQuantization quantization) {
        super();
        if (quantization == VectorQuantization.LUCENE) {
            this.vectorFormat = new LuceneKnnScalarQuantizedVectorFormatV2(maxDimensions);
        } else {
            this.vectorFormat = new LuceneKnnVectorFormatV2(maxDimensions);
        }
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return vectorFormat;
    }
}
