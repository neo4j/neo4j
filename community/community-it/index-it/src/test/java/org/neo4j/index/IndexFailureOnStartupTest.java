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
package org.neo4j.index;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.index.SabotageNativeIndex.nativeIndexDirectoryStructure;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

@DbmsExtension
@ExtendWith(RandomExtension.class)
public class IndexFailureOnStartupTest {
    private static final Label PERSON = Label.label("Person");

    @Inject
    private RandomSupport random;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private DbmsController controller;

    @Inject
    private FileSystemAbstraction fs;

    @Test
    void failedIndexShouldRepairAutomatically() {
        // given
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(PERSON).on("name").create();
            tx.commit();
        }
        awaitIndexesOnline(5, SECONDS);
        createNamed(PERSON, "Johan");
        // when - we restart the database in a state where the index is not operational
        sabotageNativeIndexAndRestartDbms();
        // then - the database should still be operational
        createNamed(PERSON, "Lars");
        awaitIndexesOnline(5, SECONDS);
        indexStateShouldBe(ONLINE);
        assertFindsNamed(PERSON, "Lars");
    }

    @Test
    void shouldNotBeAbleToViolateConstraintWhenBackingIndexFailsToOpen() {
        // given
        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(PERSON).assertPropertyIsUnique("name").create();
            tx.commit();
        }
        createNamed(PERSON, "Lars");
        // when - we restart the database in a state where the index is not operational
        sabotageNativeIndexAndRestartDbms();
        // then - we must not be able to violate the constraint
        createNamed(PERSON, "Johan");
        // this must fail, otherwise we have violated the constraint
        assertThrows(ConstraintViolationException.class, () -> createNamed(PERSON, "Lars"));
        indexStateShouldBe(ONLINE);
    }

    @Nested
    @DbmsExtension(configurationCallback = "configure")
    class ArchiveIndex {

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            builder.setConfig(GraphDatabaseInternalSettings.archive_failed_index, true);
        }

        @Test
        void shouldArchiveFailedIndex() throws IOException {
            // given
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode(PERSON);
                node.setProperty("name", "Fry");
                tx.commit();
            }
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode(PERSON);
                node.setProperty("name", Values.pointValue(CoordinateReferenceSystem.WGS_84, 1, 2));
                tx.commit();
            }

            try (Transaction tx = db.beginTx()) {
                tx.schema().constraintFor(PERSON).assertPropertyIsUnique("name").create();
                tx.commit();
            }
            assertThat(archiveFile()).isNull();

            // when
            sabotageNativeIndexAndRestartDbms();
            // then
            indexStateShouldBe(ONLINE);
            assertThat(archiveFile()).isNotNull();
        }
    }

    private void sabotageNativeIndexAndRestartDbms() {
        var openOptions = db.getDependencyResolver()
                .resolveDependency(StorageEngine.class)
                .getOpenOptions();
        controller.restartDbms(db.databaseName(), builder -> {
            try {
                new SabotageNativeIndex(random.random(), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                        .run(fs, db.databaseLayout(), openOptions);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return builder;
        });
    }

    private Path archiveFile() throws IOException {
        Path indexDir = nativeIndexDirectoryStructure(db.databaseLayout(), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .rootDirectory();
        Path[] files;
        try (Stream<Path> list = Files.list(indexDir)) {
            files = list.filter(path -> Files.isRegularFile(path)
                            && path.getFileName().toString().startsWith("archive-"))
                    .toArray(Path[]::new);
        }
        if (files.length == 0) {
            return null;
        }
        assertEquals(1, files.length);
        return files[0];
    }

    private void awaitIndexesOnline(int timeout, TimeUnit unit) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(timeout, unit);
            tx.commit();
        }
    }

    private void assertFindsNamed(Label label, String name) {
        try (Transaction tx = db.beginTx()) {
            assertNotNull(
                    tx.findNode(label, "name", name), "Must be able to find node created while index was offline");
            tx.commit();
        }
    }

    private void indexStateShouldBe(Schema.IndexState value) {
        try (Transaction tx = db.beginTx()) {
            for (IndexDefinition index : tx.schema().getIndexes()) {
                assertThat(tx.schema().getIndexState(index)).isEqualTo(value);
            }
            tx.commit();
        }
    }

    private void createNamed(Label label, String name) {
        try (Transaction tx = db.beginTx()) {
            tx.createNode(label).setProperty("name", name);
            tx.commit();
        }
    }
}
