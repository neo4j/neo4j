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
package org.neo4j.kernel.api.impl.schema.fulltext;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.SkipOnSpd;

/**
 * Tests testing affect of TX state on index query results.
 * <p>
 * Results of index queries must combine the content of the index
 * with relevant changes in the TX state and that is what is tested here.
 */
@ExtendWith(OtherThreadExtension.class)
class FulltextIndexTransactionStateTest extends FulltextProceduresTestSupport {
    @Inject
    private OtherThread otherThread;

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void queryResultFromTransactionStateMustSortTogetherWithResultFromBaseIndex(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        String firstId;
        String secondId;
        String thirdId;

        try (Transaction tx = db.beginTx()) {
            firstId = entityUtil.createEntityWithProperty(tx, "God of War");
            thirdId = entityUtil.createEntityWithProperty(tx, "God Wars: Future Past");
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            secondId = entityUtil.createEntityWithProperty(tx, "God of War III Remastered");
            entityUtil.assertQueryFindsIdsInOrder(tx, "god of war", firstId, secondId, thirdId);
            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void queryResultsMustIncludeEntitiesAddedInTheSameTransaction(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        try (Transaction tx = db.beginTx()) {
            String id = entityUtil.createEntityWithProperty(tx, "value");
            entityUtil.assertQueryFindsIdsInOrder(tx, "value", id);
            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    @SkipOnSpd(reason = "SPD full text ties are not ordered by entity id ( possibly adapt test)")
    void queryResultsMustNotIncludeEntitiesDeletedInTheSameTransaction(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        String entityIdA;
        String entityIdB;
        try (Transaction tx = db.beginTx()) {
            entityIdA = entityUtil.createEntityWithProperty(tx, "value");
            entityIdB = entityUtil.createEntityWithProperty(tx, "value");
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            var bothEntitiesResult = new String[] {entityIdA, entityIdB};
            Arrays.sort(bothEntitiesResult);
            entityUtil.assertQueryFindsIdsInOrder(tx, "value", bothEntitiesResult);

            entityUtil.deleteEntity(tx, entityIdA);
            entityUtil.assertQueryFindsIdsInOrder(tx, "value", entityIdB);

            entityUtil.deleteEntity(tx, entityIdB);
            entityUtil.assertQueryFindsIdsInOrder(tx, "value");
            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    @SkipOnSpd(reason = "constantly timing out, to be investigated")
    void queryingIndexInPopulatingStateMustBlockUntilIndexIsOnlineEvenWhenTransactionHasState(EntityUtil entityUtil)
            throws InterruptedException {
        trapPopulation.set(true);

        try (Transaction tx = db.beginTx()) {
            entityUtil.createEntityWithProperty(tx, "value");
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            entityUtil.createIndex(tx);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            entityUtil.createEntityWithProperty(tx, "value");
            try (var resultStream = entityUtil.queryIndex(tx, "value").stream()) {
                populationScanFinished.await();
                populationScanFinished.release();
                assertThat(resultStream.count()).isEqualTo(2);
            }
            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void queryResultsMustIncludeOldPropertyValuesWhenModificationsAreUndone(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        String entityId;
        try (Transaction tx = db.beginTx()) {
            entityId = entityUtil.createEntityWithProperty(tx, "primo");
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Entity entity = entityUtil.getEntity(tx, entityId);
            entityUtil.assertQueryFindsIdsInOrder(tx, "primo", entityId);
            entityUtil.assertQueryFindsIdsInOrder(tx, "secundo");
            entity.setProperty(PROP, "secundo");
            entityUtil.assertQueryFindsIdsInOrder(tx, "primo");
            entityUtil.assertQueryFindsIdsInOrder(tx, "secundo", entityId);
            entity.setProperty(PROP, "primo");
            entityUtil.assertQueryFindsIdsInOrder(tx, "primo", entityId);
            entityUtil.assertQueryFindsIdsInOrder(tx, "secundo");
            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void queryResultsMustIncludeOldPropertyValuesWhenRemovalsAreUndone(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        String entityId;
        try (Transaction tx = db.beginTx()) {
            entityId = entityUtil.createEntityWithProperty(tx, "primo");
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            entityUtil.assertQueryFindsIdsInOrder(tx, "primo", entityId);
            Entity entity = entityUtil.getEntity(tx, entityId);
            entity.removeProperty(PROP);
            entityUtil.assertQueryFindsIdsInOrder(tx, "primo");
            entity.setProperty(PROP, "primo");
            entityUtil.assertQueryFindsIdsInOrder(tx, "primo", entityId);
            tx.commit();
        }
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void transactionStateMustNotPreventIndexUpdatesFromBeingApplied(EntityUtil entityUtil) throws Exception {
        createIndexAndWait(entityUtil);
        Set<String> entityIds = new HashSet<>();

        try (Transaction tx = db.beginTx()) {
            entityIds.add(entityUtil.createEntityWithProperty(tx, "value"));

            otherThread
                    .execute(() -> {
                        try (Transaction forkedTx = db.beginTx()) {
                            entityIds.add(entityUtil.createEntityWithProperty(tx, "value"));
                            forkedTx.commit();
                        }

                        return null;
                    })
                    .get();
            tx.commit();
        }
        entityUtil.assertQueryFindsIds(db, "value", entityIds);
    }

    @MethodSource("entityTypeProvider")
    @ParameterizedTest
    void fulltextIndexMustWorkAfterRestartWithTxStateChanges(EntityUtil entityUtil) {
        createIndexAndWait(entityUtil);

        restartDatabase();

        try (Transaction tx = db.beginTx()) {
            // create an indexed entity ...
            String id = entityUtil.createEntityWithProperty(tx, "value");
            // ... and not indexed one
            entityUtil.createEntity(tx);
            entityUtil.assertQueryFindsIdsInOrder(tx, "*", id);
            tx.commit();
        }
    }

    @MethodSource("entityTypeAndIndexProvider")
    @ParameterizedTest
    void transactionStateShouldWorkOnAllFulltextIndexProviders(
            EntityUtil entityUtil, IndexProviderDescriptor indexProvider) throws KernelException {
        try (Transaction tx = db.beginTx()) {
            entityUtil.createIndexWithProvider(tx, indexProvider);
            tx.commit();
        }
        awaitIndexesOnline();

        String firstId;
        String secondId;
        String thirdId;

        try (Transaction tx = db.beginTx()) {
            firstId = entityUtil.createEntityWithProperty(tx, "God of War");
            thirdId = entityUtil.createEntityWithProperty(tx, "God Wars: Future Past");
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            secondId = entityUtil.createEntityWithProperty(tx, "God of War III Remastered");
            entityUtil.assertQueryFindsIdsInOrder(tx, "god of war", firstId, secondId, thirdId);
            tx.commit();
        }
    }

    static Stream<Arguments> entityTypeAndIndexProvider() {
        return AllIndexProviderDescriptors.INDEX_TYPES.entrySet().stream()
                .filter(e -> e.getValue() == IndexType.FULLTEXT)
                .map(Entry::getKey)
                .flatMap(indexProvider ->
                        entityTypeProvider().map(entityUtil -> Arguments.of(entityUtil, indexProvider)));
    }
}
