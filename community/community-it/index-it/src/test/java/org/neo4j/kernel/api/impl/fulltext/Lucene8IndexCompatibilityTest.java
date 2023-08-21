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
package org.neo4j.kernel.api.impl.fulltext;

import static java.io.OutputStream.nullOutputStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unordered;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.fulltextSearch;

import java.io.PrintStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.MigrateStoreCommand;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.tags.MultiVersionedTag;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

/**
 * Tests that indexes created by Neo 4.x (4.4 concretely) which used Lucene 8.x
 * work with Neo 5.x+ which uses Lucene 9.x.
 * <p>
 * This test check basic readability and writeability of Lucene indexes created with Lucene 8.
 * Lucene contains backwards compatible codecs, so the main purpose of this test is checking
 * that Lucene is configured correctly to use these codecs.
 * <p>
 * It uses a database created by Neo 4.4 which contains one fulltext and one text index created
 * over five nodes with the following text properties:
 * <ul>
 *     <li>Text 0 written by 4.4</li>
 *     <li>Text 1 written by 4.4</li>
 *     <li>Text 2 written by 4.4</li>
 *     <li>Text 3 written by 4.4</li>
 *     <li>Text 4 written by 4.4</li>
 * </ul>
 */
@TestDirectoryExtension
@MultiVersionedTag
class Lucene8IndexCompatibilityTest {
    private static final String ARCHIVE_NAME = "4-4-lucene-idx-db.zip";
    private static final String TEXT_IDX_NAME = "TextIndex";
    private static final String FULLTEXT_IDX_NAME = "FulltextIndex";
    private static final String LABEL = "Label";
    private static final String PROPERTY = "property";

    @Inject
    TestDirectory testDirectory;

    private DatabaseManagementService dbms;
    private GraphDatabaseService db;

    @BeforeEach
    void beforeEach() throws Exception {
        Unzip.unzip(getClass(), ARCHIVE_NAME, testDirectory.homePath());

        runStoreMigrationCommandFromSameJvm();
        dbms = new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build();
        db = dbms.database("neo4j");
    }

    @AfterEach
    void tearDown() {
        if (dbms != null) {
            dbms.shutdown();
        }
    }

    @Test
    void testExistingIndexesWork() throws KernelException {
        assertTextIndexContainsExactlyValues(
                "Text 0 written by 4.4",
                "Text 1 written by 4.4",
                "Text 2 written by 4.4",
                "Text 3 written by 4.4",
                "Text 4 written by 4.4");

        assertFulltextIndexContainsExactlyValues(
                "Text 0 written by 4.4",
                "Text 1 written by 4.4",
                "Text 2 written by 4.4",
                "Text 3 written by 4.4",
                "Text 4 written by 4.4");

        try (var tx = db.beginTx()) {
            // modify existing:
            var node1 = tx.findNode(Label.label(LABEL), PROPERTY, "Text 1 written by 4.4");
            node1.setProperty(PROPERTY, "New text 1");

            // delete existing:
            var node2 = tx.findNode(Label.label(LABEL), PROPERTY, "Text 3 written by 4.4");
            node2.delete();

            // create new:
            var node3 = tx.createNode(Label.label(LABEL));
            node3.setProperty(PROPERTY, "New text 2");

            tx.commit();
        }

        assertTextIndexContainsExactlyValues(
                "Text 0 written by 4.4", "Text 2 written by 4.4", "Text 4 written by 4.4", "New text 1", "New text 2");

        assertFulltextIndexContainsExactlyValues(
                "Text 0 written by 4.4", "Text 2 written by 4.4", "Text 4 written by 4.4", "New text 1", "New text 2");
    }

    private void assertTextIndexContainsExactlyValues(String... values) throws KernelException {
        try (var tx = db.beginTx()) {
            var kernelTx = ((TransactionImpl) tx).kernelTransaction();

            IndexDescriptor index = kernelTx.schemaRead().indexGetForName(TEXT_IDX_NAME);
            IndexReadSession indexSession = kernelTx.dataRead().indexReadSession(index);

            try (NodeValueIndexCursor cursor = kernelTx.cursors()
                    .allocateNodeValueIndexCursor(kernelTx.cursorContext(), kernelTx.memoryTracker())) {
                kernelTx.dataRead()
                        .nodeIndexSeek(
                                kernelTx.queryContext(),
                                indexSession,
                                cursor,
                                unordered(false),
                                PropertyIndexQuery.allEntries());
                int entryCounter = 0;
                while (cursor.next()) {
                    entryCounter++;
                }

                assertEquals(values.length, entryCounter);
            }
            int prop = kernelTx.tokenRead().propertyKey(PROPERTY);

            for (String value : values) {

                try (NodeValueIndexCursor cursor = kernelTx.cursors()
                        .allocateNodeValueIndexCursor(kernelTx.cursorContext(), kernelTx.memoryTracker())) {
                    var predicate = PropertyIndexQuery.exact(prop, value);
                    kernelTx.dataRead()
                            .nodeIndexSeek(kernelTx.queryContext(), indexSession, cursor, unordered(false), predicate);
                    assertTrue(cursor.next());
                }
            }
        }
    }

    private void assertFulltextIndexContainsExactlyValues(String... values) throws KernelException {
        try (var tx = db.beginTx()) {
            var kernelTx = ((TransactionImpl) tx).kernelTransaction();

            IndexDescriptor index = kernelTx.schemaRead().indexGetForName(FULLTEXT_IDX_NAME);
            IndexReadSession indexSession = kernelTx.dataRead().indexReadSession(index);

            try (NodeValueIndexCursor cursor = kernelTx.cursors()
                    .allocateNodeValueIndexCursor(kernelTx.cursorContext(), kernelTx.memoryTracker())) {
                kernelTx.dataRead()
                        .nodeIndexSeek(
                                kernelTx.queryContext(), indexSession, cursor, unordered(false), fulltextSearch("*"));
                int entryCounter = 0;
                while (cursor.next()) {
                    entryCounter++;
                }

                assertEquals(values.length, entryCounter);
            }

            for (String value : values) {

                try (NodeValueIndexCursor cursor = kernelTx.cursors()
                        .allocateNodeValueIndexCursor(kernelTx.cursorContext(), kernelTx.memoryTracker())) {
                    kernelTx.dataRead()
                            .nodeIndexSeek(
                                    kernelTx.queryContext(),
                                    indexSession,
                                    cursor,
                                    unordered(false),
                                    fulltextSearch(value));
                    assertTrue(cursor.next());
                }
            }
        }
    }

    private void runStoreMigrationCommandFromSameJvm() throws Exception {
        var homeDir = testDirectory.homePath().toAbsolutePath();
        var configDir = homeDir.resolve(Config.DEFAULT_CONFIG_DIR_NAME);

        var nullOut = new PrintStream(nullOutputStream());
        var ctx = new ExecutionContext(homeDir, configDir, nullOut, nullOut, new DefaultFileSystemAbstraction());
        var command = CommandLine.populateCommand(new MigrateStoreCommand(ctx), "*");
        command.call();
    }
}
