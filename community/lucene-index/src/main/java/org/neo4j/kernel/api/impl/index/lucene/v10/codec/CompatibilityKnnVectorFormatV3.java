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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import org.apache.lucene.backward_codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

public class CompatibilityKnnVectorFormatV3 extends KnnVectorsFormat {
    private static final String FORMAT_NAME = "CompatibilityKnnVectorFormatV3";

    private static final KnnVectorsFormat NONE = new Lucene99HnswVectorsFormat();
    private static final KnnVectorsFormat SCALAR = new Lucene99HnswScalarQuantizedVectorsFormat();

    /// Same as `Lucene99HnswVectorsFormat#META_EXTENSION` which is package-private
    private static final String NONE_META_EXTENSION = "vem";

    /// Same as `Lucene99ScalarQuantizedVectorsFormat.META_EXTENSION` which is package-private
    private static final String SCALAR_META_EXTENSION = "vemq";

    /// Used by Lucene Service Loader for reading from segments
    public CompatibilityKnnVectorFormatV3() {
        super(FORMAT_NAME);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) {
        throw new IllegalStateException("The %s must not be used for writing, only for reading"
                .formatted(getClass().getSimpleName()));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        // Value is only used during indexing, where this class is not used
        return Integer.MAX_VALUE;
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        if (fileExists(state, SCALAR_META_EXTENSION)) {
            return SCALAR.fieldsReader(state);
        }
        assert fileExists(state, NONE_META_EXTENSION) : "Neither SCALAR or NONE files exist in the index";
        return NONE.fieldsReader(state);
    }

    private static boolean fileExists(SegmentReadState state, String extension) throws IOException {
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
        try {
            long ignoreFileSize = state.directory.fileLength(fileName);
            return true;
        } catch (NoSuchFileException | FileNotFoundException e) {
            return false;
        }
    }
}
