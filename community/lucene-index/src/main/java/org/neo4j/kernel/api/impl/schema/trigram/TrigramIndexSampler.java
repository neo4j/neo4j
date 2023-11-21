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
package org.neo4j.kernel.api.impl.schema.trigram;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;

@SuppressWarnings("ClassCanBeRecord")
public class TrigramIndexSampler implements IndexSampler {
    private final IndexSearcher indexSearcher;

    public TrigramIndexSampler(IndexSearcher indexSearcher) {
        this.indexSearcher = indexSearcher;
    }

    @Override
    public IndexSample sampleIndex(CursorContext cursorContext, AtomicBoolean stopped)
            throws IndexNotFoundKernelException {
        // This way of sampling will not provide a correct estimate for the number of unique value.
        // Getting the number of unique values in a trigram index is really difficult so instead of
        // for example getting an estimate by reading from the store or storing some extra information
        // in the index itself, we consider the index size to be good enough.
        IndexReader indexReader = indexSearcher.getIndexReader();
        var numDocs = indexReader.numDocs();
        return new IndexSample(numDocs, numDocs, numDocs);
    }
}
