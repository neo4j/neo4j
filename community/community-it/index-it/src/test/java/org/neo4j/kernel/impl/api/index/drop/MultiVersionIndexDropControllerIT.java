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
package org.neo4j.kernel.impl.api.index.drop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.stream;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.TransactionVisibilityProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class MultiVersionIndexDropControllerIT {
    @Inject
    JobScheduler jobScheduler;

    @Inject
    IndexingService indexingService;

    @Inject
    GraphDatabaseService database;

    @Inject
    IndexProviderMap providerMap;

    @Inject
    FileSystemAbstraction fileSystem;

    private MultiVersionIndexDropController indexDropController;

    @AfterEach
    void tearDown() {
        if (indexDropController != null) {
            indexDropController.stop();
        }
    }

    @Test
    void doNotDropIndexesWhileTheyAreVisible() {
        Label label = Label.label("marker");
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().indexFor(label).on("property1").create();
            transaction.schema().indexFor(label).on("property2").create();
            transaction.schema().indexFor(label).on("property3").create();
            transaction.commit();
        }

        awaitIndexes();

        var indexDescriptors = getIndexDescriptors(label);
        var indexProvider = getIndexProvider(indexDescriptors);

        indexDropController = new MultiVersionIndexDropController(
                jobScheduler,
                new TestTransactionVisibilityProvider(10, 100),
                indexingService,
                fileSystem,
                NullLogProvider.getInstance());
        indexDropController.start();

        for (IndexDescriptor indexDescriptor : indexDescriptors) {
            indexDropController.dropIndex(indexDescriptor);
        }

        indexDropController.dropIndexes();

        assertThat(indexDropController.getAsyncDeleteQueue()).hasSize(3);

        var directoryStructure = indexProvider.directoryStructure();
        for (IndexDescriptor indexDescriptor : indexDescriptors) {
            Path indexDirectory = directoryStructure.directoryForIndex(indexDescriptor.getId());
            assertTrue(
                    fileSystem.fileExists(indexDirectory),
                    () -> "Directory " + indexDirectory + " should still exist.");
        }
    }

    @Test
    void dropIndexesOnlyWhenTheyAreInvisible() {
        Label label = Label.label("marker");
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().indexFor(label).on("property1").create();
            transaction.schema().indexFor(label).on("property2").create();
            transaction.schema().indexFor(label).on("property3").create();
            transaction.commit();
        }

        awaitIndexes();

        var indexDescriptors = getIndexDescriptors(label);
        var indexProvider = getIndexProvider(indexDescriptors);

        AtomicLong oldestValue = new AtomicLong(5);
        indexDropController = new MultiVersionIndexDropController(
                jobScheduler,
                new TestTransactionVisibilityProvider(
                        new AtomicStateLongSupplier(oldestValue), new ArrayStateLongSupplier(new long[] {10, 20, 30})),
                indexingService,
                fileSystem,
                NullLogProvider.getInstance());

        indexDropController.start();

        for (IndexDescriptor indexDescriptor : indexDescriptors) {
            indexDropController.dropIndex(indexDescriptor);
        }

        // nothing was dropped
        indexDropController.dropIndexes();
        assertThat(indexDropController.getAsyncDeleteQueue()).hasSize(3);

        oldestValue.set(11);

        // one index is dropped now
        indexDropController.dropIndexes();
        assertThat(indexDropController.getAsyncDeleteQueue()).hasSize(2);

        oldestValue.set(22);

        // two indexes dropped
        indexDropController.dropIndexes();
        assertThat(indexDropController.getAsyncDeleteQueue()).hasSize(1);

        var directoryStructure = indexProvider.directoryStructure();
        assertFalse(fileSystem.fileExists(
                directoryStructure.directoryForIndex(indexDescriptors.get(0).getId())));
        assertFalse(fileSystem.fileExists(
                directoryStructure.directoryForIndex(indexDescriptors.get(1).getId())));
        // last index is not removed yet
        assertTrue(fileSystem.fileExists(
                directoryStructure.directoryForIndex(indexDescriptors.get(2).getId())));
    }

    @Test
    void errorOnDropDoesNotBlockDropQueue() {
        Label label = Label.label("marker");
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().indexFor(label).on("property1").create();
            transaction.schema().indexFor(label).on("property2").create();
            transaction.schema().indexFor(label).on("property3").create();
            transaction.commit();
        }

        awaitIndexes();

        var indexDescriptors = getIndexDescriptors(label);

        AssertableLogProvider logProvider = new AssertableLogProvider();
        indexDropController = new MultiVersionIndexDropController(
                jobScheduler, new TestTransactionVisibilityProvider(11, 10), indexingService, fileSystem, logProvider);
        indexDropController.start();

        for (IndexDescriptor indexDescriptor : indexDescriptors) {
            indexDropController.dropIndex(indexDescriptor);
        }
        indexDropController.dropIndexes();
        assertThat(indexDropController.getAsyncDeleteQueue()).hasSize(0);
        LogAssertions.assertThat(logProvider).doesNotContainMessage("Exception on multi version index async drop.");

        for (IndexDescriptor indexDescriptor : indexDescriptors) {
            indexDropController.dropIndex(indexDescriptor);
        }
        indexDropController.dropIndexes();

        assertThat(indexDropController.getAsyncDeleteQueue()).hasSize(0);
        LogAssertions.assertThat(logProvider).containsMessages("Exception on multi version index async drop.");
    }

    @Test
    void oldestIndexesDroppedBeforeNewlyAdded() {
        Label label = Label.label("marker");
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().indexFor(label).on("property1").create();
            transaction.schema().indexFor(label).on("property2").create();
            transaction.schema().indexFor(label).on("property3").create();
            transaction.commit();
        }

        awaitIndexes();

        var indexDescriptors = getIndexDescriptors(label);

        AtomicLong oldestValue = new AtomicLong(5);
        indexDropController = new MultiVersionIndexDropController(
                jobScheduler,
                new TestTransactionVisibilityProvider(
                        new AtomicStateLongSupplier(oldestValue), new ArrayStateLongSupplier(new long[] {6, 7, 8})),
                indexingService,
                fileSystem,
                NullLogProvider.getInstance());

        indexDropController.start();

        for (IndexDescriptor indexDescriptor : indexDescriptors) {
            indexDropController.dropIndex(indexDescriptor);
            oldestValue.incrementAndGet();
        }
        indexDropController.dropIndexes();

        assertThat(indexDropController.getAsyncDeleteQueue()).hasSize(1);
    }

    private IndexProvider getIndexProvider(List<IndexDescriptor> indexDescriptors) {
        return providerMap.lookup(indexDescriptors.get(0).getIndexProvider());
    }

    private List<IndexDescriptor> getIndexDescriptors(Label label) {
        try (Transaction tx = database.beginTx()) {
            return stream(tx.schema().getIndexes(label))
                    .map(indexDefinition -> ((IndexDefinitionImpl) indexDefinition).getIndexReference())
                    .toList();
        }
    }

    private void awaitIndexes() {
        try (Transaction tx = database.beginTx()) {
            tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES);
        }
    }

    private static class TestTransactionVisibilityProvider implements TransactionVisibilityProvider {
        private final long youngestBoundary;
        private final long oldestBoundary;
        private final LongSupplier oldestBoundarySupplier;
        private final LongSupplier youngestBoundarySupplier;

        public TestTransactionVisibilityProvider(long oldestBoundary, long youngestBoundary) {
            this(oldestBoundary, youngestBoundary, null, null);
        }

        public TestTransactionVisibilityProvider(
                LongSupplier oldestBoundarySupplier, LongSupplier youngestBoundarySupplier) {
            this(0, 0, oldestBoundarySupplier, youngestBoundarySupplier);
        }

        private TestTransactionVisibilityProvider(
                long oldestBoundary,
                long youngestBoundary,
                LongSupplier oldestBoundarySupplier,
                LongSupplier youngestBoundarySupplier) {
            this.youngestBoundary = youngestBoundary;
            this.oldestBoundary = oldestBoundary;
            this.oldestBoundarySupplier = oldestBoundarySupplier;
            this.youngestBoundarySupplier = youngestBoundarySupplier;
        }

        @Override
        public long oldestVisibleClosedTransactionId() {
            return 0;
        }

        @Override
        public long oldestObservableHorizon() {
            return oldestBoundarySupplier == null ? oldestBoundary : oldestBoundarySupplier.getAsLong();
        }

        @Override
        public long youngestObservableHorizon() {
            return youngestBoundarySupplier == null ? youngestBoundary : youngestBoundarySupplier.getAsLong();
        }
    }

    private static class ArrayStateLongSupplier implements LongSupplier {
        private final long[] states;
        private int index;

        public ArrayStateLongSupplier(long[] states) {
            this.states = states;
        }

        @Override
        public long getAsLong() {
            return states[index++];
        }
    }

    private record AtomicStateLongSupplier(AtomicLong oldestValue) implements LongSupplier {
        @Override
        public long getAsLong() {
            return oldestValue.get();
        }
    }
}
