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

import static org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory.ENTITY_ID_KEY;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.schema.writer.LucenePartitionIndexWriter;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;

/**
 * A {@link TextIndexPopulatingUpdater} used for non-unique Lucene schema indexes.
 */
public class TextIndexPopulatingUpdater implements IndexUpdater {

    private final LucenePartitionIndexWriter writer;
    private final IndexUpdateIgnoreStrategy ignoreStrategy;
    private final LuceneDocumentsFactory documentsFactory;

    public TextIndexPopulatingUpdater(LucenePartitionIndexWriter writer, IndexUpdateIgnoreStrategy ignoreStrategy) {
        this.writer = writer;
        this.ignoreStrategy = ignoreStrategy;
        documentsFactory = writer.documentsFactory();
    }

    @Override
    public void close() {
        // writer is not closed here as it's shared with the populator
    }

    @Override
    public void process(IndexEntryUpdate update) {
        ValueIndexEntryUpdate valueUpdate = asValueUpdate(update);
        if (valueUpdate == null) {
            return;
        }

        try {
            long entityId = valueUpdate.getEntityId();
            Value[] values = valueUpdate.values();
            UpdateMode updateMode = valueUpdate.updateMode();
            switch (updateMode) {
                case ADDED ->
                    writer.updateDocument(
                            ENTITY_ID_KEY, entityId, documentsFactory.reusableTextDocument(entityId, values));
                case CHANGED ->
                    writer.updateOrDeleteDocument(
                            ENTITY_ID_KEY, entityId, documentsFactory.reusableTextDocument(entityId, values));
                case REMOVED -> writer.deleteDocuments(ENTITY_ID_KEY, entityId);
                default ->
                    throw new IllegalStateException(
                            "Unknown update mode " + updateMode + " for values " + Arrays.toString(values));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ValueIndexEntryUpdate asValueUpdate(IndexEntryUpdate update) {
        ValueIndexEntryUpdate valueUpdate = IndexUpdater.super.asValueUpdate(update);
        return !ignoreStrategy.ignore(valueUpdate) ? ignoreStrategy.toEquivalentUpdate(valueUpdate) : null;
    }
}
