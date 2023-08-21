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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.api.impl.LuceneTestUtil.documentRepresentingProperties;
import static org.neo4j.kernel.api.impl.schema.TextDocumentStructure.newTermForChangeOrRemove;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.api.index.IndexQueryHelper.change;
import static org.neo4j.kernel.api.index.IndexQueryHelper.remove;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexProvider;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

@TestDirectoryExtension
class TextIndexPopulatingTest {
    private static final SchemaDescriptorSupplier SCHEMA_DESCRIPTOR = () -> SchemaDescriptors.forLabel(1, 42);

    @Test
    void additionsDeliveredToIndexWriter() throws Exception {
        LuceneIndexWriter writer = mock(LuceneIndexWriter.class);
        TextIndexPopulatingUpdater updater = newUpdater(writer);

        updater.process(add(1, SCHEMA_DESCRIPTOR, "foo"));
        verify(writer).updateDocument(newTermForChangeOrRemove(1), documentRepresentingProperties(1, "foo"));

        updater.process(add(2, SCHEMA_DESCRIPTOR, "bar"));
        verify(writer).updateDocument(newTermForChangeOrRemove(2), documentRepresentingProperties(2, "bar"));

        updater.process(add(3, SCHEMA_DESCRIPTOR, "qux"));
        verify(writer).updateDocument(newTermForChangeOrRemove(3), documentRepresentingProperties(3, "qux"));
    }

    @Test
    void changesDeliveredToIndexWriter() throws Exception {
        LuceneIndexWriter writer = mock(LuceneIndexWriter.class);
        TextIndexPopulatingUpdater updater = newUpdater(writer);

        updater.process(change(1, SCHEMA_DESCRIPTOR, "before1", "after1"));
        verify(writer).updateOrDeleteDocument(newTermForChangeOrRemove(1), documentRepresentingProperties(1, "after1"));

        updater.process(change(2, SCHEMA_DESCRIPTOR, "before2", "after2"));
        verify(writer).updateOrDeleteDocument(newTermForChangeOrRemove(2), documentRepresentingProperties(2, "after2"));
    }

    @Test
    void removalsDeliveredToIndexWriter() throws Exception {
        LuceneIndexWriter writer = mock(LuceneIndexWriter.class);
        TextIndexPopulatingUpdater updater = newUpdater(writer);

        updater.process(remove(1, SCHEMA_DESCRIPTOR, "foo"));
        verify(writer).deleteDocuments(newTermForChangeOrRemove(1));

        updater.process(remove(2, SCHEMA_DESCRIPTOR, "bar"));
        verify(writer).deleteDocuments(newTermForChangeOrRemove(2));

        updater.process(remove(3, SCHEMA_DESCRIPTOR, "baz"));
        verify(writer).deleteDocuments(newTermForChangeOrRemove(3));
    }

    private static TextIndexPopulatingUpdater newUpdater(LuceneIndexWriter writer) {
        return new TextIndexPopulatingUpdater(writer, AbstractTextIndexProvider.UPDATE_IGNORE_STRATEGY);
    }
}
