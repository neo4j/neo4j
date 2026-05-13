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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.lucene.LuceneContext;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.text.TextIndexBuilder;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class TextIndexTest {
    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory testDir;

    private DirectoryFactory dirFactory;
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

    @ParameterizedTest
    @EnumSource
    void markAsOnline(LuceneContext luceneContext) throws IOException {
        index = createIndex(luceneContext);
        index.getIndexWriter().addDocument(newDocument(luceneContext));
        index.markAsOnline();

        assertTrue(index.isOnline(), "Should have had online status set");
    }

    @ParameterizedTest
    @EnumSource
    void markAsOnlineAndClose(LuceneContext luceneContext) throws IOException {
        index = createIndex(luceneContext);
        index.getIndexWriter().addDocument(newDocument(luceneContext));
        index.markAsOnline();

        index.close();

        index = openIndex(luceneContext);
        assertTrue(index.isOnline(), "Should have had online status set");
    }

    @ParameterizedTest
    @EnumSource
    void markAsOnlineTwice(LuceneContext luceneContext) throws IOException {
        index = createIndex(luceneContext);
        index.markAsOnline();

        index.getIndexWriter().addDocument(newDocument(luceneContext));
        index.markAsOnline();

        assertTrue(index.isOnline(), "Should have had online status set");
    }

    @ParameterizedTest
    @EnumSource
    void markAsOnlineTwiceAndClose(LuceneContext luceneContext) throws IOException {
        index = createIndex(luceneContext);
        index.markAsOnline();

        index.getIndexWriter().addDocument(newDocument(luceneContext));
        index.markAsOnline();
        index.close();

        index = openIndex(luceneContext);
        assertTrue(index.isOnline(), "Should have had online status set");
    }

    @ParameterizedTest
    @EnumSource
    void markAsOnlineIsRespectedByOtherWriter(LuceneContext luceneContext) throws IOException {
        index = createIndex(luceneContext);
        index.markAsOnline();
        index.close();

        index = openIndex(luceneContext);
        index.getIndexWriter().addDocument(newDocument(luceneContext));
        index.close();

        index = openIndex(luceneContext);
        assertTrue(index.isOnline(), "Should have had online status set");
    }

    private DatabaseIndex<ValueIndexReader> createIndex(LuceneContext luceneContext) throws IOException {
        dirFactory = DirectoryFactory.inMemory(luceneContext);
        DatabaseIndex<ValueIndexReader> schemaIndex = newSchemaIndex(luceneContext);
        schemaIndex.create();
        schemaIndex.open();
        return schemaIndex;
    }

    private DatabaseIndex<ValueIndexReader> openIndex(LuceneContext luceneContext) throws IOException {
        DatabaseIndex<ValueIndexReader> schemaIndex = newSchemaIndex(luceneContext);
        schemaIndex.open();
        return schemaIndex;
    }

    private DatabaseIndex<ValueIndexReader> newSchemaIndex(LuceneContext luceneContext) {
        TextIndexBuilder builder =
                TextIndexBuilder.create(descriptor, writable(), Config.defaults(), NullLogProvider.getInstance());
        return builder.withIndexRootFolder(testDir.directory("index").resolve("testIndex"))
                .withDirectoryFactory(dirFactory)
                .withLuceneContext(luceneContext)
                .withFileSystem(fs)
                .build();
    }

    private static LuceneDocument newDocument(LuceneContext luceneContext) {
        LuceneDocument doc = luceneContext.documentsFactory().newDocument();
        doc.addStringField("test", UUID.randomUUID().toString(), true);
        return doc;
    }
}
