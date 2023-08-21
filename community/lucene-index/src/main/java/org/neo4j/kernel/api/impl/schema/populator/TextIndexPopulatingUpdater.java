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
package org.neo4j.kernel.api.impl.schema.populator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.kernel.api.impl.schema.TextDocumentStructure;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;

/**
 * A {@link TextIndexPopulatingUpdater} used for non-unique Lucene schema indexes.
 */
public class TextIndexPopulatingUpdater implements IndexUpdater {

    private final LuceneIndexWriter writer;
    private final IndexUpdateIgnoreStrategy ignoreStrategy;

    public TextIndexPopulatingUpdater(LuceneIndexWriter writer, IndexUpdateIgnoreStrategy ignoreStrategy) {
        this.writer = writer;
        this.ignoreStrategy = ignoreStrategy;
    }

    @Override
    public void close() {
        // writer is not closed here as it's shared with the populator
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
            final var updateMode = valueUpdate.updateMode();
            switch (updateMode) {
                case ADDED -> writer.updateDocument(
                        TextDocumentStructure.newTermForChangeOrRemove(entityId),
                        TextDocumentStructure.documentRepresentingProperties(entityId, values));
                case CHANGED -> writer.updateOrDeleteDocument(
                        TextDocumentStructure.newTermForChangeOrRemove(entityId),
                        TextDocumentStructure.documentRepresentingProperties(entityId, values));
                case REMOVED -> writer.deleteDocuments(TextDocumentStructure.newTermForChangeOrRemove(entityId));
                default -> throw new IllegalStateException(
                        "Unknown update mode " + updateMode + " for values " + Arrays.toString(values));
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
}
