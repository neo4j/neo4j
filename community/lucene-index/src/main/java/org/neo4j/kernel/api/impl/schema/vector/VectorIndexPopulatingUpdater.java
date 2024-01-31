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

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarityFunctions.LuceneVectorSimilarityFunction;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.vector.VectorCandidate;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;

class VectorIndexPopulatingUpdater implements IndexUpdater {
    private final LuceneIndexWriter writer;
    private final IndexUpdateIgnoreStrategy ignoreStrategy;
    private final LuceneVectorSimilarityFunction similarityFunction;

    VectorIndexPopulatingUpdater(
            LuceneIndexWriter writer,
            IndexUpdateIgnoreStrategy ignoreStrategy,
            LuceneVectorSimilarityFunction similarityFunction) {
        this.writer = writer;
        this.ignoreStrategy = ignoreStrategy;
        this.similarityFunction = similarityFunction;
    }

    @Override
    public void process(IndexEntryUpdate<?> update) {
        final var valueUpdate = asValueUpdate(update);
        if (valueUpdate == null) {
            return;
        }

        try {
            final var entityId = valueUpdate.getEntityId();
            final var values = valueUpdate.values();
            final var candidate = VectorCandidate.maybeFrom(values[0]);
            final var updateMode = valueUpdate.updateMode();
            switch (updateMode) {
                case ADDED -> writer.updateDocument(
                        VectorDocumentStructure.newTermForChangeOrRemove(entityId),
                        VectorDocumentStructure.createLuceneDocument(entityId, candidate, similarityFunction));
                case CHANGED -> writer.updateOrDeleteDocument(
                        VectorDocumentStructure.newTermForChangeOrRemove(entityId),
                        VectorDocumentStructure.createLuceneDocument(entityId, candidate, similarityFunction));
                case REMOVED -> writer.deleteDocuments(VectorDocumentStructure.newTermForChangeOrRemove(entityId));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public <INDEX_KEY extends SchemaDescriptorSupplier> ValueIndexEntryUpdate<INDEX_KEY> asValueUpdate(
            IndexEntryUpdate<INDEX_KEY> update) {
        final var valueUpdate = IndexUpdater.super.asValueUpdate(update);
        return !ignoreStrategy.ignore(valueUpdate) ? ignoreStrategy.toEquivalentUpdate(valueUpdate) : null;
    }

    @Override
    public void close() throws IndexEntryConflictException {}
}
