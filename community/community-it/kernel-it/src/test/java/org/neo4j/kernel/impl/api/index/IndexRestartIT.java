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
package org.neo4j.kernel.impl.api.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class IndexRestartIT {
    private static final String myKey = "number_of_bananas_owned";
    private static final Label myLabel = label("MyLabel");

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    private GraphDatabaseService db;
    private TestDatabaseManagementServiceBuilder factory;
    private final ControlledPopulationIndexProvider provider = new ControlledPopulationIndexProvider();
    private DatabaseManagementService managementService;

    @BeforeEach
    void before() {
        factory = new TestDatabaseManagementServiceBuilder(directory.homePath());
        factory.setFileSystem(new UncloseableDelegatingFileSystemAbstraction(fs));
        factory.addExtension(singleInstanceIndexProviderFactory("test", provider));
    }

    @AfterEach
    void after() {
        managementService.shutdown();
    }

    /* This is somewhat difficult to test since dropping an index while it's populating forces it to be cancelled
     * first (and also awaiting cancellation to complete). So this is a best-effort to have the timing as close
     * as possible. If this proves to be flaky, remove it right away.
     */
    @Test
    void shouldBeAbleToDropIndexWhileItIsPopulating() throws InterruptedException, KernelException {
        // GIVEN
        startDb();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 10; i++) {
                tx.createNode(myLabel).setProperty(myKey, i);
            }
            tx.commit();
        }
        Barrier.Control barrier =
                provider.installPopulationLatch(ControlledPopulationIndexProvider.PopulationLatchMethod.ADD_BATCH);
        IndexDefinition index = createIndex();
        barrier.await();

        // WHEN
        dropIndex(index, barrier);
        try (Transaction transaction = db.beginTx()) {
            // THEN
            assertThat(getIndexes(transaction, myLabel)).isEmpty();
            var e = assertThrows(NotFoundException.class, () -> indexState(transaction, index));
            assertThat(e).hasMessageContaining(myLabel.name());
        }
    }

    @Test
    void shouldHandleRestartOfOnlineIndex() throws KernelException {
        // Given
        startDb();
        createIndex();
        provider.awaitFullyPopulated();

        // And Given
        stopDb();
        provider.setInitialIndexState(ONLINE);

        // When
        startDb();

        // Then
        try (Transaction transaction = db.beginTx()) {
            var indexes = getIndexes(transaction, myLabel);
            for (IndexDefinition definition : indexes) {
                assertThat(indexState(transaction, definition)).isEqualTo(Schema.IndexState.ONLINE);
            }
        }
        assertEquals(1, provider.populatorCallCount.get());
        assertEquals(2, provider.writerCallCount.get());
    }

    @Test
    void shouldHandleRestartIndexThatHasNotComeOnlineYet() throws KernelException {
        // Given
        startDb();
        createIndex();

        // And Given
        stopDb();
        provider.setInitialIndexState(POPULATING);

        // When
        startDb();

        try (Transaction transaction = db.beginTx()) {
            var indexes = getIndexes(transaction, myLabel);
            for (IndexDefinition definition : indexes) {
                assertThat(indexState(transaction, definition)).isNotEqualTo(Schema.IndexState.FAILED);
            }
        }
        assertEquals(2, provider.populatorCallCount.get());
    }

    private IndexDefinition createIndex() throws KernelException {
        try (TransactionImpl tx = (TransactionImpl) db.beginTx()) {
            IndexingTestUtil.createNodePropIndexWithSpecifiedProvider(
                    tx, provider.getProviderDescriptor(), myLabel, myKey);
            tx.commit();
        }

        // Return the IndexDefinition for the index instead since that is what we want later
        try (Transaction transaction = db.beginTx()) {
            return Iterables.first(getIndexes(transaction, myLabel));
        }
    }

    private void dropIndex(IndexDefinition index, Barrier.Control populationCompletionLatch) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getIndexByName(index.getName()).drop();
            populationCompletionLatch.release();
            tx.commit();
        }
    }

    private void startDb() {
        if (managementService != null) {
            managementService.shutdown();
        }

        managementService = factory.noOpSystemGraphInitializer().build();
        db = managementService.database(DEFAULT_DATABASE_NAME);
    }

    private void stopDb() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    private static Iterable<IndexDefinition> getIndexes(Transaction transaction, Label label) {
        return transaction.schema().getIndexes(label);
    }

    private static Schema.IndexState indexState(Transaction transaction, IndexDefinition index) {
        return transaction.schema().getIndexState(index);
    }
}
