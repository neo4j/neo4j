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
package org.neo4j.kernel.impl.newapi.index;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.eclipse.collections.impl.block.factory.Functions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestBase;
import org.neo4j.kernel.impl.newapi.ReadTestSupport;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class EntityValueIndexCursorLimitedIndexTest {
    @Nested
    final class Point extends IndexSuite {

        Point() {
            super(IndexType.POINT);
        }

        @Override
        protected Stream<Value> validValues() {
            return Stream.of(
                    Values.pointValue(CoordinateReferenceSystem.WGS_84, -122.322312, 37.563437),
                    Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.994807, 55.612088),
                    Values.pointValue(CoordinateReferenceSystem.WGS_84, -0.101008, 51.503773),
                    Values.pointValue(CoordinateReferenceSystem.WGS_84, 11.572188, 48.135813));
        }

        @Override
        protected Stream<Value> invalidValues() {
            return numbers();
        }
    }

    @Nested
    final class Text extends IndexSuite {
        Text() {
            super(IndexType.TEXT);
        }

        @Override
        protected Stream<Value> validValues() {
            return Stream.of("abc", "def", "uvw", "xyz").map(Values::of);
        }

        @Override
        protected Stream<Value> invalidValues() {
            return numbers();
        }
    }

    private static Stream<Value> numbers() {
        return Stream.of(-999, -99, 99, 999).map(Values::of);
    }

    abstract static class IndexSuite {
        private final IndexType type;

        IndexSuite(IndexType type) {
            this.type = type;
        }

        protected abstract Stream<Value> validValues();

        protected abstract Stream<Value> invalidValues();

        @Nested
        final class Node extends Entity<NodeValueIndexCursor> {
            Node() {
                super(IndexSuite.this, new NodeParams());
            }
        }

        @Nested
        final class Relationship extends Entity<RelationshipValueIndexCursor> {
            Relationship() {
                super(IndexSuite.this, new RelationshipParams());
            }
        }
    }

    @ExtendWith(SoftAssertionsExtension.class)
    abstract static class Entity<ENTITY_VALUE_INDEX_CURSOR extends Cursor & ValueIndexCursor>
            extends KernelAPIReadTestBase<ReadTestSupport> {
        private static final String ENTITY_TOKEN = "EntityToken";
        private static final String PROPERTY_KEY = "PropKey";
        private static final String INDEX_NAME = "Index";

        private final IndexSuite suite;
        private final EntityParams<ENTITY_VALUE_INDEX_CURSOR> entityParams;

        private Set<Long> validCommittedEntityIds;
        private Set<Long> invalidCommittedEntityIds;

        @InjectSoftAssertions
        private SoftAssertions softly;

        Entity(IndexSuite suite, EntityParams<ENTITY_VALUE_INDEX_CURSOR> entityParams) {
            this.suite = suite;
            this.entityParams = entityParams;
        }

        @Override
        public ReadTestSupport newTestSupport() {
            return new ReadTestSupport();
        }

        @Override
        public void createTestGraph(GraphDatabaseService db) {
            try (var tx = db.beginTx()) {
                entityParams.createEntityIndex(tx, ENTITY_TOKEN, PROPERTY_KEY, INDEX_NAME, suite.type);
                tx.commit();
            }

            try (var ktx = beginTransaction()) {
                validCommittedEntityIds = createEntitiesWithProp(ktx, suite.validValues());
                invalidCommittedEntityIds = createEntitiesWithProp(ktx, suite.invalidValues());
                ktx.commit();
            } catch (Exception e) {
                throw new AssertionError("failed to create graph", e);
            }

            try (var tx = db.beginTx()) {
                tx.schema().awaitIndexesOnline(5, TimeUnit.MINUTES);
            }
        }

        @Test
        final void scanWithoutTxStateShouldOnlyReturnCommitedValidValuesForIndex() throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                // given  an index with a type which does not store all value types, and already committed valid indexed
                // entities
                // when   index scanned without transaction state
                // then   should find only the already committed entities
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx, entities, validCommittedEntityIds, "should find only expected committed entries");
            }
        }

        @Test
        final void scanWithValidTxStateShouldOnlyReturnValidCommitedValuesAndStateForIndex() throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                // given  transaction state of valid value types
                final var expected = new HashSet<>(validCommittedEntityIds);
                expected.addAll(createEntitiesWithProp(tx, suite.validValues()));

                // when   index scanned
                // then   should find both the already committed entries, and the valid state entries
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx, entities, expected, "should find both committed entries, and the valid state entries");
            }
        }

        @Test
        final void scanWithInvalidTxStateShouldOnlyReturnValidCommitedValuesForIndex() throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                // given  transaction state of invalid value types
                createEntitiesWithProp(tx, suite.invalidValues());

                // when   index scanned
                // then   should not find invalid state entries
                //        should find the already committed entries
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx, entities, validCommittedEntityIds, "should find only committed entries");
            }
        }

        @Test
        final void scanWithValid2ValidChangeTxStateShouldOnlyReturnValidCommitedValuesForIndexWithChange()
                throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                // given  a change of commited entry: valid -> valid
                final var commitedPool = new ArrayDeque<>(validCommittedEntityIds);
                changePropValue(
                        tx,
                        commitedPool.removeLast(),
                        suite.validValues().findFirst().orElseThrow());

                // when   index scanned
                // then   should find the commited entries, including the changed entry
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx,
                        entities,
                        validCommittedEntityIds,
                        "should find commited entries, including the changed entry");
            }
        }

        @Test
        final void scanWithValid2InvalidChangeTxStateShouldOnlyReturnValidCommitedValuesForIndexWithoutChange()
                throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                // given  a change of commited entry: valid -> invalid
                final var commitedPool = new ArrayDeque<>(validCommittedEntityIds);
                changePropValue(
                        tx,
                        commitedPool.removeLast(),
                        suite.invalidValues().findFirst().orElseThrow());

                // when   index scanned
                // then   should not find the changed entry
                //        should find the other commited entries
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx, entities, commitedPool, "should find commited entries, without the changed entry");
            }
        }

        @Test
        final void scanWithInvalid2ValidChangeTxStateShouldOnlyReturnValidCommitedValuesForIndexWithChange()
                throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                // given  a change of commited entry: invalid -> valid
                final var expected = new HashSet<>(validCommittedEntityIds);
                final var commitedPool = new ArrayDeque<>(invalidCommittedEntityIds);
                expected.add(changePropValue(
                        tx,
                        commitedPool.removeLast(),
                        suite.validValues().findFirst().orElseThrow()));

                // when   index scanned
                // then   should find the commited entries, including the changed entry
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx, entities, expected, "should find commited entries, including the changed entry");
            }
        }

        @Test
        final void scanWithInvalid2InvalidChangeTxStateShouldOnlyReturnValidCommitedValuesForIndexWithoutChange()
                throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                // given  a change of commited entry: invalid -> invalid
                final var commitedPool = new ArrayDeque<>(invalidCommittedEntityIds);
                changePropValue(
                        tx,
                        commitedPool.removeLast(),
                        suite.invalidValues().findFirst().orElseThrow());

                // when   index scanned
                // then   should have no change, returning commited entries
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx,
                        entities,
                        validCommittedEntityIds,
                        "should find commited entries, without the changed entry");
            }
        }

        @Test
        final void scanWithRemovedValidTxStateShouldOnlyReturnValidCommitedValuesForIndexWithoutRemoved()
                throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                // given  a removal of a valid entry
                final var commitedPool = new ArrayDeque<>(validCommittedEntityIds);
                removeEntity(tx, commitedPool.removeLast());

                // when   index scanned
                // then   should not find the removed entry
                //        should find the other commited entries
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx, entities, commitedPool, "should find commited entries, without the removed entry");
            }
        }

        @Test
        final void scanWithRemovedInvalidTxStateShouldOnlyReturnValidCommitedValuesForIndexWithoutRemoved()
                throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                // given  a removal of a invalid entry
                final var commitedPool = new ArrayDeque<>(invalidCommittedEntityIds);
                removeEntity(tx, commitedPool.removeLast());

                // when   index scanned
                // then   should have no change, returning commited entries
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx,
                        entities,
                        validCommittedEntityIds,
                        "should find commited entries, without the removed entry");
            }
        }

        @Test
        final void scanWithComplexTxStateShouldOnlyReturnValidCommitedAndStateForIndex() throws KernelException {
            try (var tx = beginTransaction();
                    var entities = entityParams.allocateEntityValueIndexCursor(tx, tx.cursors())) {
                final var expected = new HashSet<>(validCommittedEntityIds);
                final var validCommitedPool = new ArrayDeque<>(validCommittedEntityIds);
                final var invalidCommitedPool = new ArrayDeque<>(invalidCommittedEntityIds);

                // given  a complex mix of changing commited entries, and creating new entries
                expected.addAll(createEntitiesWithProp(tx, suite.validValues()));
                createEntitiesWithProp(tx, suite.invalidValues());
                changePropValue(
                        tx,
                        validCommitedPool.removeLast(),
                        suite.validValues().findFirst().orElseThrow());
                expected.remove(changePropValue(
                        tx,
                        validCommitedPool.removeLast(),
                        suite.invalidValues().findFirst().orElseThrow()));
                expected.add(changePropValue(
                        tx,
                        invalidCommitedPool.removeLast(),
                        suite.validValues().findFirst().orElseThrow()));
                changePropValue(
                        tx,
                        invalidCommitedPool.removeLast(),
                        suite.invalidValues().findFirst().orElseThrow());
                expected.remove(removeEntity(tx, validCommitedPool.removeLast()));
                removeEntity(tx, invalidCommitedPool.removeLast());

                // when   index scanned
                // then   should find all valid entries
                assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                        tx, entities, expected, "should find all valid entries");
            }
        }

        private long createEntityWithProp(KernelTransaction tx, Value value) throws KernelException {
            final var tokenId = entityParams.entityTokenId(tx, ENTITY_TOKEN);
            final var propKeyId = tx.tokenWrite().propertyKeyGetOrCreateForName(PROPERTY_KEY);
            final var entityId = entityParams.entityCreateNew(tx, tokenId);
            entityParams.entitySetProperty(tx, entityId, propKeyId, value);
            return entityId;
        }

        private Set<Long> createEntitiesWithProp(KernelTransaction tx, Stream<Value> values) {
            return values.map(Functions.throwing(value -> createEntityWithProp(tx, value)))
                    .collect(Collectors.toUnmodifiableSet());
        }

        private long changePropValue(KernelTransaction tx, long entityId, Value value) throws KernelException {
            final var propKeyId = tx.tokenWrite().propertyKeyGetOrCreateForName(PROPERTY_KEY);
            entityParams.entitySetProperty(tx, entityId, propKeyId, value);
            return entityId;
        }

        private long removeEntity(KernelTransaction tx, long entityId) throws KernelException {
            entityParams.entityDelete(tx, entityId);
            return entityId;
        }

        private void assertThatIndexScanContainsExactlyInAnyOrderElementsOf(
                KernelTransaction tx, ENTITY_VALUE_INDEX_CURSOR entities, Iterable<Long> expected, String description)
                throws KernelException {
            final var index = tx.schemaRead().indexGetForName(INDEX_NAME);
            final var session = tx.dataRead().indexReadSession(index);

            entityParams.entityIndexScan(tx, session, entities, IndexQueryConstraints.unconstrained());
            final var found = new HashSet<Long>();
            while (entities.next()) {
                found.add(entityParams.entityReference(entities));
            }

            softly.assertThat(found).as(description).containsExactlyInAnyOrderElementsOf(expected);
        }
    }
}
