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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.api.index.ControlledPopulationIndexProvider;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension(configurationCallback = "configure")
public class SchemaIndexWaitingAcceptanceTest {
    @Inject
    private GraphDatabaseService database;

    private final ControlledPopulationIndexProvider provider = new ControlledPopulationIndexProvider();

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.addExtension(singleInstanceIndexProviderFactory("test", provider))
                .noOpSystemGraphInitializer();
    }

    @BeforeEach
    void createSomeData() {
        try (Transaction tx = database.beginTx()) {
            tx.createNode();
            tx.commit();
        }
    }

    @Test
    void shouldTimeoutWaitingForIndexToComeOnline() throws InterruptedException, KernelException {
        // given
        Barrier.Control barrier =
                provider.installPopulationLatch(ControlledPopulationIndexProvider.PopulationLatchMethod.CREATE);

        IndexDefinition index;
        try (TransactionImpl tx = (TransactionImpl) database.beginTx()) {
            IndexingTestUtil.createNodePropIndexWithSpecifiedProvider(
                    tx, provider.getProviderDescriptor(), Label.label("Person"), "name");

            index = Iterables.single(tx.schema().getIndexes(Label.label("Person")));
            tx.commit();
        }

        barrier.await();

        var e = assertThrows(IllegalStateException.class, () -> {
            try (Transaction tx = database.beginTx()) {
                tx.schema().awaitIndexOnline(index, 1, TimeUnit.MILLISECONDS);
            }
        });
        assertThat(e).hasMessageContaining("come online");
        barrier.release();
    }

    @Test
    void shouldTimeoutWaitingForIndexByNameToComeOnline() throws InterruptedException, KernelException {
        // given
        Barrier.Control barrier =
                provider.installPopulationLatch(ControlledPopulationIndexProvider.PopulationLatchMethod.CREATE);

        try (TransactionImpl tx = (TransactionImpl) database.beginTx()) {
            IndexingTestUtil.createNodePropIndexWithSpecifiedProvider(
                    tx, provider.getProviderDescriptor(), Label.label("Person"), "name", "my_index");
            tx.commit();
        }

        barrier.await();

        var e = assertThrows(IllegalStateException.class, () -> {
            try (Transaction tx = database.beginTx()) {
                tx.schema().awaitIndexOnline("my_index", 1, TimeUnit.MILLISECONDS);
            }
        });
        assertThat(e).hasMessageContaining("come online");
        barrier.release();
    }

    @Test
    void shouldTimeoutWaitingForAllIndexesToComeOnline() throws InterruptedException, KernelException {
        // given
        Barrier.Control barrier =
                provider.installPopulationLatch(ControlledPopulationIndexProvider.PopulationLatchMethod.CREATE);

        try (TransactionImpl tx = (TransactionImpl) database.beginTx()) {
            IndexingTestUtil.createNodePropIndexWithSpecifiedProvider(
                    tx, provider.getProviderDescriptor(), Label.label("Person"), "name");
            tx.commit();
        }

        barrier.await();

        // when
        var e = assertThrows(IllegalStateException.class, () -> {
            try (Transaction tx = database.beginTx()) {
                tx.schema().awaitIndexesOnline(1, TimeUnit.MILLISECONDS);
            }
        });
        assertThat(e).hasMessageContaining("come online");
        barrier.release();
    }
}
