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

import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;

public class LuceneKnnVectorFormatV1 extends KnnVectorsFormat {
    private static final String LUCENE_VECTOR_FORMAT_V1_NAME = "LuceneKnnVectorFormatV1";
    private final Lucene95HnswVectorsFormat vectorsFormat;
    private final int maxDimensions;

    public LuceneKnnVectorFormatV1() {
        this(VectorIndexVersion.V1_0.maxDimensions());
    }

    public LuceneKnnVectorFormatV1(int maxDimensions) {
        super(LUCENE_VECTOR_FORMAT_V1_NAME);
        this.maxDimensions = maxDimensions;
        this.vectorsFormat = new Lucene95HnswVectorsFormat();
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return vectorsFormat.fieldsWriter(state);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return vectorsFormat.fieldsReader(state);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return maxDimensions;
    }
}
