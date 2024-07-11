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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarityFunctions.LuceneVectorSimilarityFunction;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.vector.VectorCandidate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.values.storable.Value;

class VectorIndexAccessor extends AbstractLuceneIndexAccessor<VectorIndexReader, DatabaseIndex<VectorIndexReader>> {
    private final VectorDocumentStructure documentStructure;
    private final LuceneVectorSimilarityFunction similarityFunction;

    protected VectorIndexAccessor(
            DatabaseIndex<VectorIndexReader> luceneIndex,
            IndexDescriptor descriptor,
            IndexUpdateIgnoreStrategy ignoreStrategy,
            VectorDocumentStructure documentStructure,
            LuceneVectorSimilarityFunction similarityFunction) {
        super(luceneIndex, descriptor, ignoreStrategy);
        this.documentStructure = documentStructure;
        this.similarityFunction = similarityFunction;
    }

    @Override
    public BoundedIterable<Long> newAllEntriesValueReader(
            long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
        try {
            return luceneIndex
                    .getIndexReader(NO_USAGE_TRACKING)
                    .newAllEntriesValueReader(fromIdInclusive, toIdExclusive);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected IndexUpdater getIndexUpdater(IndexUpdateMode mode) {
        return new VectorIndexUpdater(mode.requiresIdempotency(), mode.requiresRefresh());
    }

    private class VectorIndexUpdater extends AbstractLuceneIndexUpdater {

        VectorIndexUpdater(boolean idempotent, boolean refresh) {
            super(idempotent, refresh);
        }

        @Override
        protected void addIdempotent(long entityId, Value[] values) {
            try {
                final var document = documentStructure.createLuceneDocument(
                        entityId, VectorCandidate.maybeFrom(values[0]), similarityFunction);
                writer.updateOrDeleteDocument(VectorDocumentStructure.newTermForChangeOrRemove(entityId), document);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void add(long entityId, Value[] values) {
            try {
                final var document = documentStructure.createLuceneDocument(
                        entityId, VectorCandidate.maybeFrom(values[0]), similarityFunction);
                writer.nullableAddDocument(document);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void change(long entityId, Value[] values) {
            try {
                final var term = VectorDocumentStructure.newTermForChangeOrRemove(entityId);
                final var document = documentStructure.createLuceneDocument(
                        entityId, VectorCandidate.maybeFrom(values[0]), similarityFunction);
                writer.updateOrDeleteDocument(term, document);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void remove(long entityId) {
            try {
                final var term = VectorDocumentStructure.newTermForChangeOrRemove(entityId);
                writer.deleteDocuments(term);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
