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
package org.neo4j.kernel.impl.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.LogTestUtils.filterNeostoreLogicalLog;

import java.io.IOException;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.LogTestUtils.CountingLogHook;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

/**
 * Asserts that pure read operations does not write records to logical or transaction logs.
 */
@ImpermanentDbmsExtension
class ReadTransactionLogWritingTest {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private LogFiles logFiles;

    @Inject
    private FileSystemAbstraction fileSystem;

    private final Label label = label("Test");
    private Node node;
    private Relationship relationship;
    private long logEntriesWrittenBeforeReadOperations;

    @BeforeEach
    void createDataset() {
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode(label);
            node.setProperty("short", 123);
            node.setProperty("long", longString(300));
            relationship = node.createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
            relationship.setProperty("short", 123);
            relationship.setProperty("long", longString(300));
            tx.commit();
        }
        logEntriesWrittenBeforeReadOperations = countLogEntries();
    }

    @Test
    void shouldNotWriteAnyLogCommandInPureReadTransaction() {
        // WHEN
        executeTransaction(getRelationships());
        executeTransaction(getProperties());
        executeTransaction(getById());
        executeTransaction(getNodesFromRelationship());

        // THEN
        long actualCount = countLogEntries();
        assertEquals(
                logEntriesWrittenBeforeReadOperations,
                actualCount,
                "There were " + (actualCount - logEntriesWrittenBeforeReadOperations)
                        + " log entries written during one or more pure read transactions");
    }

    private long countLogEntries() {
        try {
            CountingLogHook<LogEntry> logicalLogCounter = new CountingLogHook<>();
            filterNeostoreLogicalLog(
                    logFiles,
                    fileSystem,
                    logicalLogCounter,
                    db.getDependencyResolver()
                            .resolveDependency(StorageEngineFactory.class)
                            .commandReaderFactory());

            long txLogRecordCount =
                    logFiles.getLogFile().getLogFileInformation().getLastEntryAppendIndex();

            return logicalLogCounter.getCount() + txLogRecordCount;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String longString(int length) {
        char[] characters = new char[length];
        for (int i = 0; i < length; i++) {
            characters[i] = (char) ('a' + i % 10);
        }
        return new String(characters);
    }

    private void executeTransaction(Consumer<Transaction> consumer) {
        executeTransaction(consumer, true);
        executeTransaction(consumer, false);
    }

    private void executeTransaction(Consumer<Transaction> consumer, boolean success) {
        try (Transaction tx = db.beginTx()) {
            consumer.accept(tx);
            if (success) {
                tx.commit();
            }
        }
    }

    private Consumer<Transaction> getRelationships() {
        return tx ->
                assertEquals(1, Iterables.count(tx.getNodeById(node.getId()).getRelationships()));
    }

    private Consumer<Transaction> getNodesFromRelationship() {
        return tx -> {
            var rel = tx.getRelationshipById(relationship.getId());
            rel.getEndNode();
            rel.getStartNode();
            rel.getNodes();
            rel.getOtherNode(node);
        };
    }

    private Consumer<Transaction> getById() {
        return tx -> {
            tx.getNodeById(node.getId());
            tx.getRelationshipById(relationship.getId());
        };
    }

    private Consumer<Transaction> getProperties() {
        return new Consumer<>() {
            @Override
            public void accept(Transaction tx) {
                getAllProperties(tx.getNodeById(node.getId()));
                getAllProperties(tx.getRelationshipById(relationship.getId()));
            }

            private void getAllProperties(Entity entity) {
                for (String key : entity.getPropertyKeys()) {
                    entity.getProperty(key);
                }
            }
        };
    }
}
