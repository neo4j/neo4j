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
package org.neo4j.kernel.api.impl.schema;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;

import java.io.IOException;
import java.util.UUID;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class TextIndexTest {
    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory testDir;

    private final DirectoryFactory dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private DatabaseIndex<ValueIndexReader> index;
    private final IndexDescriptor descriptor = IndexPrototype.forSchema(forLabel(3, 5))
            .withName("a")
            .withIndexType(IndexType.TEXT)
            .withIndexProvider(AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR)
            .materialise(1);

    @AfterEach
    void closeIndex() throws Exception {
        IOUtils.closeAll(index, dirFactory);
    }

    @Test
    void markAsOnline() throws IOException {
        index = createIndex();
        index.getIndexWriter().addDocument(newDocument());
        index.markAsOnline();

        assertTrue(index.isOnline(), "Should have had online status set");
    }

    @Test
    void markAsOnlineAndClose() throws IOException {
        index = createIndex();
        index.getIndexWriter().addDocument(newDocument());
        index.markAsOnline();

        index.close();

        index = openIndex();
        assertTrue(index.isOnline(), "Should have had online status set");
    }

    @Test
    void markAsOnlineTwice() throws IOException {
        index = createIndex();
        index.markAsOnline();

        index.getIndexWriter().addDocument(newDocument());
        index.markAsOnline();

        assertTrue(index.isOnline(), "Should have had online status set");
    }

    @Test
    void markAsOnlineTwiceAndClose() throws IOException {
        index = createIndex();
        index.markAsOnline();

        index.getIndexWriter().addDocument(newDocument());
        index.markAsOnline();
        index.close();

        index = openIndex();
        assertTrue(index.isOnline(), "Should have had online status set");
    }

    @Test
    void markAsOnlineIsRespectedByOtherWriter() throws IOException {
        index = createIndex();
        index.markAsOnline();
        index.close();

        index = openIndex();
        index.getIndexWriter().addDocument(newDocument());
        index.close();

        index = openIndex();
        assertTrue(index.isOnline(), "Should have had online status set");
    }

    private DatabaseIndex<ValueIndexReader> createIndex() throws IOException {
        var schemaIndex = newSchemaIndex();
        schemaIndex.create();
        schemaIndex.open();
        return schemaIndex;
    }

    private DatabaseIndex<ValueIndexReader> openIndex() throws IOException {
        var schemaIndex = newSchemaIndex();
        schemaIndex.open();
        return schemaIndex;
    }

    private DatabaseIndex<ValueIndexReader> newSchemaIndex() {
        TextIndexBuilder builder = TextIndexBuilder.create(descriptor, writable(), Config.defaults());
        return builder.withIndexRootFolder(testDir.directory("index").resolve("testIndex"))
                .withDirectoryFactory(dirFactory)
                .withFileSystem(fs)
                .build();
    }

    private static Document newDocument() {
        Document doc = new Document();
        doc.add(new StringField("test", UUID.randomUUID().toString(), Field.Store.YES));
        return doc;
    }
}
