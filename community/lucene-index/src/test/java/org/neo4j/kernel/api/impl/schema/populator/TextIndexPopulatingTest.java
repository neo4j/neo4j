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
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory.ENTITY_ID_KEY;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;
import static org.neo4j.kernel.api.index.IndexQueryHelper.change;
import static org.neo4j.kernel.api.index.IndexQueryHelper.remove;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.schema.AbstractTextIndexProvider;
import org.neo4j.kernel.api.impl.schema.writer.LucenePartitionIndexWriter;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class TextIndexPopulatingTest {
    private static final IndexDescriptor INDEX_DESCRIPTOR = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 42))
            .withName("12")
            .materialise(13);

    @ParameterizedTest
    @EnumSource
    void additionsDeliveredToIndexWriter(LuceneContext luceneContext) throws Exception {
        LuceneDocumentsFactory documentsFactory = luceneContext.documentsFactory();
        LucenePartitionIndexWriter writer = mock(LucenePartitionIndexWriter.class);
        when(writer.documentsFactory()).thenReturn(documentsFactory);

        TextIndexPopulatingUpdater updater = newUpdater(writer);

        updater.process(add(1, INDEX_DESCRIPTOR, "foo"));
        verify(writer).updateDocument(ENTITY_ID_KEY, 1, documentsFactory.reusableTextDocument(1, Values.values("foo")));

        updater.process(add(2, INDEX_DESCRIPTOR, "bar"));
        verify(writer).updateDocument(ENTITY_ID_KEY, 2, documentsFactory.reusableTextDocument(2, Values.values("bar")));

        updater.process(add(3, INDEX_DESCRIPTOR, "qux"));
        verify(writer).updateDocument(ENTITY_ID_KEY, 3, documentsFactory.reusableTextDocument(3, Values.values("qux")));
    }

    @ParameterizedTest
    @EnumSource
    void changesDeliveredToIndexWriter(LuceneContext luceneContext) throws Exception {
        LuceneDocumentsFactory documentsFactory = luceneContext.documentsFactory();
        LucenePartitionIndexWriter writer = mock(LucenePartitionIndexWriter.class);
        when(writer.documentsFactory()).thenReturn(documentsFactory);

        TextIndexPopulatingUpdater updater = newUpdater(writer);

        updater.process(change(1, INDEX_DESCRIPTOR, "before1", "after1"));
        verify(writer)
                .updateOrDeleteDocument(
                        ENTITY_ID_KEY, 1, documentsFactory.reusableTextDocument(1, Values.values("after1")));

        updater.process(change(2, INDEX_DESCRIPTOR, "before2", "after2"));
        verify(writer)
                .updateOrDeleteDocument(
                        ENTITY_ID_KEY, 2, documentsFactory.reusableTextDocument(2, Values.values("after2")));
    }

    @ParameterizedTest
    @EnumSource
    void removalsDeliveredToIndexWriter(LuceneContext luceneContext) throws Exception {
        LuceneDocumentsFactory documentsFactory = luceneContext.documentsFactory();
        LucenePartitionIndexWriter writer = mock(LucenePartitionIndexWriter.class);
        when(writer.documentsFactory()).thenReturn(documentsFactory);
        TextIndexPopulatingUpdater updater = newUpdater(writer);

        updater.process(remove(1, INDEX_DESCRIPTOR, "foo"));
        verify(writer).deleteDocuments(ENTITY_ID_KEY, 1);

        updater.process(remove(2, INDEX_DESCRIPTOR, "bar"));
        verify(writer).deleteDocuments(ENTITY_ID_KEY, 2);

        updater.process(remove(3, INDEX_DESCRIPTOR, "baz"));
        verify(writer).deleteDocuments(ENTITY_ID_KEY, 3);
    }

    private static TextIndexPopulatingUpdater newUpdater(LucenePartitionIndexWriter writer) {
        return new TextIndexPopulatingUpdater(writer, AbstractTextIndexProvider.UPDATE_IGNORE_STRATEGY);
    }
}
