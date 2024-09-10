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

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.graphdb.schema.IndexType.POINT;
import static org.neo4j.graphdb.schema.IndexType.RANGE;
import static org.neo4j.graphdb.schema.Schema.IndexState.FAILED;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.graphdb.schema.Schema.IndexState.POPULATING;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.schema.AnyTokens;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingImpl;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.PropertyType;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.index.internal.gbptree.DynamicSizeUtil;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintWithNameAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.IndexWithNameAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.index.schema.IndexEntryTestUtil;
import org.neo4j.kernel.impl.index.schema.IndexFiles;
import org.neo4j.kernel.impl.locking.forseti.ForsetiClient;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;
import org.neo4j.util.concurrent.BinaryLatch;

@ImpermanentDbmsExtension(configurationCallback = "configure")
@ExtendWith(OtherThreadExtension.class)
class SchemaAcceptanceTest extends SchemaAcceptanceTestBase {
    public static final double[] TEST_ARRAY1 = {200.0, 200.0};
    public static final double[] TEST_ARRAY2 = {-90.0, -90.0};

    @Inject
    private DbmsController controller;

    @Inject
    protected GraphDatabaseService db;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private OtherThread otherThread;

    private final Label otherLabel = Label.label("MY_OTHER_LABEL");
    private final RelationshipType relType = RelationshipType.withName("MY_REL_TYPE");
    private final RelationshipType otherRelType = RelationshipType.withName("MY_OTHER_REL_TYPE");
    private final RelationshipType thirdRelType = RelationshipType.withName("MY_THIRD_REL_TYPE");
    private final String propertyKey = "my_property_key";
    private final String secondPropertyKey = "my_second_property_key";
    private final String nameA = "index a";
    private final String nameB = "index b";
    private final AtomicBoolean trapPopulation = new AtomicBoolean();
    private final Barrier.Control populationScanFinished = new Barrier.Control();

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        Monitors monitors = new Monitors();
        IndexMonitor.MonitorAdapter trappingMonitor = new IndexMonitor.MonitorAdapter() {
            @Override
            public void indexPopulationScanComplete() {
                if (trapPopulation.get()) {
                    populationScanFinished.reached();
                }
            }
        };
        monitors.addMonitorListener(trappingMonitor);
        builder.setMonitors(monitors);
        builder.setConfig(GraphDatabaseInternalSettings.skip_default_indexes_on_creation, true);
    }

    @Test
    void addingAnIndexingRuleShouldSucceed() {
        // WHEN
        IndexDefinition index = createIndex(db, label, propertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, label)).containsOnly(index);
        }
    }

    @Test
    void addingACompositeIndexingRuleShouldSucceed() {
        // WHEN
        IndexDefinition index = createIndex(db, label, propertyKey, secondPropertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, label)).containsOnly(index);
        }
    }

    @ParameterizedTest
    @EnumSource(AnyTokens.class)
    void addingTokenIndexRuleShouldSucceed(AnyTokens tokens) {
        // When
        IndexDefinition index = createIndex(db, tokens, null);

        // Then
        try (var tx = db.beginTx()) {
            assertThat(tx.schema().getIndexes()).containsOnly(index);
        }
    }

    @Test
    void addingNamedIndexRuleShouldSucceed() {
        // When
        IndexDefinition index = createIndex(db, "MyIndex", label, propertyKey);

        // Then
        assertThat(index.getName()).isEqualTo("MyIndex");
        try (Transaction transaction = db.beginTx()) {
            assertThat(getIndexes(transaction, label)).containsOnly(index);
        }
    }

    @ParameterizedTest
    @EnumSource(AnyTokens.class)
    void addingNamedTokenIndexRuleShouldSucceed(AnyTokens tokens) {
        // When
        IndexDefinition index = createIndex(db, tokens, "MyIndex");

        // Then
        assertThat(index.getName()).isEqualTo("MyIndex");
        try (var tx = db.beginTx()) {
            assertThat(tx.schema().getIndexes()).containsOnly(index);
        }
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfEquivalentIndexExist(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema ->
                        schema.indexFor(label).on(propertyKey).withName("name").create(),
                schema1 ->
                        schema1.indexFor(label).on(propertyKey).withName("name").create(),
                ConstraintViolationException.class);
        Class<EquivalentSchemaRuleAlreadyExistsException> expectedCause =
                EquivalentSchemaRuleAlreadyExistsException.class;
        assertExpectedException(
                exception,
                expectedCause,
                "An equivalent index already exists",
                "Index(",
                "id=",
                "name='name'",
                "type='RANGE'",
                "schema=(:MY_LABEL {my_property_key})",
                "indexProvider='range-1.0'");
    }

    @ParameterizedTest
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfEquivalentTokenIndexExist(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema -> schema.indexFor(AnyTokens.ANY_LABELS).withName("name").create(),
                schema1 ->
                        schema1.indexFor(AnyTokens.ANY_LABELS).withName("name").create(),
                ConstraintViolationException.class);
        Class<EquivalentSchemaRuleAlreadyExistsException> expectedCause =
                EquivalentSchemaRuleAlreadyExistsException.class;
        assertExpectedException(
                exception,
                expectedCause,
                "An equivalent index already exists",
                "Index(",
                "id=",
                "name='name'",
                "type='LOOKUP'",
                "schema=(:<any-labels>)",
                "indexProvider='token-lookup-1.0'");
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfEquivalentUniquenessConstraintExist(SchemaTxStrategy txStrategy) {

        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema -> schema.constraintFor(label)
                        .assertPropertyIsUnique(propertyKey)
                        .withName("name")
                        .create(),
                schema1 -> schema1.constraintFor(label)
                        .assertPropertyIsUnique(propertyKey)
                        .withName("name")
                        .create(),
                ConstraintViolationException.class);
        Class<EquivalentSchemaRuleAlreadyExistsException> expectedCause =
                EquivalentSchemaRuleAlreadyExistsException.class;
        assertExpectedException(
                exception,
                expectedCause,
                "An equivalent constraint already exists, 'Constraint( ",
                "name='name', type='UNIQUENESS', schema=(:MY_LABEL {my_property_key}), ownedIndex=");
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfSchemaAlreadyIndexedWhenCreatingIndex(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema ->
                        schema.indexFor(label).on(propertyKey).withName("name").create(),
                schema1 -> schema1.indexFor(label)
                        .on(propertyKey)
                        .withName("otherName")
                        .create(),
                ConstraintViolationException.class);
        Class<AlreadyIndexedException> expectedCause = AlreadyIndexedException.class;
        String expectedMessage = "There already exists an index (:MY_LABEL {my_property_key}).";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfSchemaAlreadyIndexedWhenCreatingUniquenessConstraint(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema ->
                        schema.indexFor(label).on(propertyKey).withName("name").create(),
                schema1 -> schema1.constraintFor(label)
                        .assertPropertyIsUnique(propertyKey)
                        .withName("otherName")
                        .create(),
                ConstraintViolationException.class);
        Class<AlreadyIndexedException> expectedCause = AlreadyIndexedException.class;
        String expectedMessage =
                "There already exists an index (:MY_LABEL {my_property_key}). A constraint cannot be created until the index has been dropped.";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfSchemaAlreadyUniquenessConstrainedWhenCreatingIndex(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema -> schema.constraintFor(label)
                        .assertPropertyIsUnique(propertyKey)
                        .withName("name")
                        .create(),
                schema1 -> schema1.indexFor(label)
                        .on(propertyKey)
                        .withName("otherName")
                        .create(),
                ConstraintViolationException.class);
        Class<AlreadyConstrainedException> expectedCause = AlreadyConstrainedException.class;
        String expectedMessage =
                "There is a uniqueness constraint on (:MY_LABEL {my_property_key}), so an index is already created that matches this.";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfSchemaAlreadyUniquenessConstrainedWhenCreatingUniquenessConstraint(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema -> schema.constraintFor(label)
                        .assertPropertyIsUnique(propertyKey)
                        .withName("name")
                        .create(),
                schema1 -> schema1.constraintFor(label)
                        .assertPropertyIsUnique(propertyKey)
                        .withName("otherName")
                        .create(),
                ConstraintViolationException.class);
        Class<AlreadyConstrainedException> expectedCause = AlreadyConstrainedException.class;
        assertExpectedException(
                exception,
                expectedCause,
                "Constraint already exists: Constraint( ",
                "name='name', type='UNIQUENESS', schema=(:MY_LABEL {my_property_key}), ownedIndex=");
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfIndexWithNameExistsWhenCreatingIndex(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema ->
                        schema.indexFor(label).on(propertyKey).withName("name").create(),
                schema1 -> schema1.indexFor(label)
                        .on(secondPropertyKey)
                        .withName("name")
                        .create(),
                ConstraintViolationException.class);
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @ParameterizedTest
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfTokenIndexWithNameExistsWhenCreatingTokenIndex(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema -> schema.indexFor(AnyTokens.ANY_LABELS).withName("name").create(),
                schema1 -> schema1.indexFor(AnyTokens.ANY_RELATIONSHIP_TYPES)
                        .withName("name")
                        .create(),
                ConstraintViolationException.class);
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @ParameterizedTest
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfPropertyIndexWithNameExistsWhenCreatingTokenIndex(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema ->
                        schema.indexFor(label).on(propertyKey).withName("name").create(),
                schema1 ->
                        schema1.indexFor(AnyTokens.ANY_LABELS).withName("name").create(),
                ConstraintViolationException.class);
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @ParameterizedTest
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfTokenIndexWithNameExistsWhenCreatingPropertyIndex(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema -> schema.indexFor(AnyTokens.ANY_LABELS).withName("name").create(),
                schema1 ->
                        schema1.indexFor(label).on(propertyKey).withName("name").create(),
                ConstraintViolationException.class);
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfIndexWithNameExistsWhenCreatingUniquenessConstraint(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema ->
                        schema.indexFor(label).on(propertyKey).withName("name").create(),
                schema1 -> schema1.constraintFor(label)
                        .assertPropertyIsUnique(secondPropertyKey)
                        .withName("name")
                        .create(),
                ConstraintViolationException.class);
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfConstraintWithNameExistsWhenCreatingIndex(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema -> schema.constraintFor(label)
                        .assertPropertyIsUnique(propertyKey)
                        .withName("name")
                        .create(),
                schema1 -> schema1.indexFor(label)
                        .on(secondPropertyKey)
                        .withName("name")
                        .create(),
                ConstraintViolationException.class);
        Class<ConstraintWithNameAlreadyExistsException> expectedCause = ConstraintWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists a constraint called 'name'.";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @ParameterizedTest()
    @EnumSource(SchemaTxStrategy.class)
    void shouldThrowIfConstraintWithNameExistsWhenCreatingUniquenessConstraint(SchemaTxStrategy txStrategy) {
        final ConstraintViolationException exception = txStrategy.execute(
                db,
                schema -> schema.constraintFor(label)
                        .assertPropertyIsUnique(propertyKey)
                        .withName("name")
                        .create(),
                schema1 -> schema1.constraintFor(label)
                        .assertPropertyIsUnique(secondPropertyKey)
                        .withName("name")
                        .create(),
                ConstraintViolationException.class);
        Class<ConstraintWithNameAlreadyExistsException> expectedCause = ConstraintWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists a constraint called 'name'.";
        assertExpectedException(exception, expectedCause, expectedMessage);
    }

    @Test
    void droppingExistingIndexRuleShouldSucceed() {
        // GIVEN
        IndexDefinition index = createIndex(db, label, propertyKey);

        // WHEN
        dropIndex(index);

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, label)).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(AnyTokens.class)
    void droppingExistingIndexRuleShouldSucceed(AnyTokens token) {
        IndexDefinition index = createIndex(db, token, null);

        dropIndex(index);

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(tx.schema().getIndexes()).isEmpty();
        }
    }

    @Test
    void droppingNonExistingIndexShouldGiveHelpfulExceptionInSameTransaction() {
        // GIVEN
        IndexDefinition index = createIndex(db, label, propertyKey);

        // WHEN
        try (Transaction tx = db.beginTx()) {
            index = getIndex(tx, index.getName());
            index.drop();
            assertThatThrownBy(index::drop)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContainingAll(
                            "Unable to drop index",
                            "Index does not exist",
                            "Index",
                            "name",
                            index.getName(),
                            "type",
                            "RANGE",
                            "schema",
                            label.name(),
                            propertyKey,
                            "indexProvider",
                            "range-1.0");
            tx.commit();
        }

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, label)).doesNotContain(index);
        }
    }

    @ParameterizedTest
    @EnumSource(AnyTokens.class)
    void droppingNonExistingIndexShouldGiveHelpfulExceptionInSameTransaction(AnyTokens token) {
        IndexDefinition index = createIndex(db, token, null);

        try (Transaction tx = db.beginTx()) {
            index = tx.schema().getIndexByName(index.getName());
            index.drop();
            assertThatThrownBy(index::drop)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Unable to drop index: Index does not exist: ");
            tx.commit();
        }

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(tx.schema().getIndexes()).doesNotContain(index);
        }
    }

    @Test
    void droppingNonExistingIndexShouldGiveHelpfulExceptionInSeparateTransactions() {
        // GIVEN
        IndexDefinition index = createIndex(db, label, propertyKey);
        dropIndex(index);

        // WHEN
        assertThatThrownBy(() -> dropIndex(index))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No index found with the name 'index_1efc11af'.");

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, label)).doesNotContain(index);
        }
    }

    @Test
    void awaitingIndexComingOnlineWorks() {
        // GIVEN

        // WHEN
        IndexDefinition index = createIndex(db, label, propertyKey);

        // PASS
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(index, 2L, TimeUnit.MINUTES);

            // THEN
            assertThat(getIndexState(tx, index)).isEqualTo(ONLINE);
        }
    }

    @ParameterizedTest
    @EnumSource(AnyTokens.class)
    void awaitingTokenIndexComingOnlineWorks(AnyTokens token) {
        IndexDefinition index = createIndex(db, token, null);

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(index, 2L, TimeUnit.MINUTES);

            assertThat(tx.schema().getIndexState(index)).isEqualTo(ONLINE);
        }
    }

    @Test
    void awaitingIndexComingOnlineByNameWorks() {
        IndexDefinition index = createIndex(db, "my_index", label, propertyKey);

        // PASS
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline("my_index", 2L, TimeUnit.MINUTES);

            // THEN
            assertThat(getIndexState(tx, index)).isEqualTo(ONLINE);
        }
    }

    @ParameterizedTest
    @EnumSource(AnyTokens.class)
    void awaitingTokenIndexComingOnlineByNameWorks(AnyTokens token) {
        IndexDefinition index = createIndex(db, token, "my_index");

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline("my_index", 2L, TimeUnit.MINUTES);

            assertThat(tx.schema().getIndexState(index)).isEqualTo(ONLINE);
        }
    }

    @Test
    void awaitingAllIndexesComingOnlineWorks() {
        // GIVEN

        // WHEN
        IndexDefinition index = createIndex(db, label, propertyKey);
        createIndex(db, label, "other_property");

        // PASS
        waitForIndex(db, index);
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2L, TimeUnit.MINUTES);

            // THEN
            assertThat(getIndexState(tx, index)).isEqualTo(ONLINE);
        }
    }

    @Test
    void awaitingAllIndexesComingOnlineWorksWhenThereIsTokenIndex() {
        IndexDefinition index1 = createIndex(db, AnyTokens.ANY_LABELS, null);
        IndexDefinition index2 = createIndex(db, AnyTokens.ANY_RELATIONSHIP_TYPES, null);

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2L, TimeUnit.MINUTES);

            assertThat(tx.schema().getIndexState(index1)).isEqualTo(ONLINE);
            assertThat(tx.schema().getIndexState(index2)).isEqualTo(ONLINE);
        }
    }

    @Test
    void shouldPopulateIndex() {
        // GIVEN
        Node node = createNode(db, propertyKey, "Neo", label);

        // create an index
        IndexDefinition index = createIndex(db, label, propertyKey);
        waitForIndex(db, index);

        // THEN
        try (Transaction transaction = db.beginTx()) {
            assertThat(findNodesByLabelAndProperty(label, propertyKey, "Neo", transaction))
                    .containsOnly(node);
        }
    }

    @Test
    void shouldPopulateTokenIndex() {
        // GIVEN
        Node node = createNode(db, propertyKey, "Neo", label);

        // create an index
        IndexDefinition index = createIndex(db, AnyTokens.ANY_LABELS, null);
        waitForIndex(db, index);

        // THEN
        try (var tx = db.beginTx()) {
            assertThat(asSet(tx.findNodes(label))).containsOnly(node);
        }
    }

    @Test
    void recreatingDroppedIndexMustProduceNewDefinition() {
        // GIVEN
        Node node = createNode(db, propertyKey, "Neo", label);

        // create an index
        IndexDefinition index = createIndex(db, label, propertyKey);
        waitForIndex(db, index);

        // delete the index right away
        dropIndex(index);

        // WHEN recreating that index
        IndexDefinition newIndex = createIndex(db, label, propertyKey);
        assertThatThrownBy(() -> waitForIndex(db, index))
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("No index was found");
        waitForIndex(db, newIndex);

        try (Transaction transaction = db.beginTx()) {
            // THEN it should exist and be usable
            assertThat(getIndexes(transaction, label)).contains(index);
            assertThat(findNodesByLabelAndProperty(label, propertyKey, "Neo", transaction))
                    .contains(node);
            transaction.commit();
        }
    }

    private static List<Node> findNodesByLabelAndProperty(
            Label label, String propertyKey, String value, Transaction transaction) {
        return Iterators.asList(transaction.findNodes(label, propertyKey, value));
    }

    @Test
    void shouldCreateUniquenessConstraint() {
        // WHEN
        ConstraintDefinition constraint = createUniquenessConstraint(label, propertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            constraint = tx.schema().getConstraintByName(constraint.getName());
            assertThat(constraint.getConstraintType()).isEqualTo(ConstraintType.UNIQUENESS);
            assertThat(constraint.getLabel().name()).isEqualTo(label.name());
            assertThat(constraint.getPropertyKeys()).containsExactly(propertyKey);
            assertThat(constraint.getName()).isEqualTo("constraint_d3208c60");
            tx.commit();
        }
    }

    @Test
    void shouldCreateRelUniquenessConstraint() {
        // WHEN
        ConstraintDefinition constraint = createRelUniquenessConstraint(relType, propertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            constraint = tx.schema().getConstraintByName(constraint.getName());
            assertThat(constraint.getConstraintType()).isEqualTo(ConstraintType.RELATIONSHIP_UNIQUENESS);
            assertThat(constraint.getRelationshipType().name()).isEqualTo(relType.name());
            assertThat(constraint.getPropertyKeys()).containsExactly(propertyKey);
            assertThat(constraint.getName()).isEqualTo("constraint_c5954bea");
            tx.commit();
        }
    }

    @Test
    void shouldCreateNamedUniquenessConstraint() {
        // When
        ConstraintDefinition constraint = createUniquenessConstraint("MyConstraint", label, propertyKey);

        // Then
        try (Transaction tx = db.beginTx()) {
            constraint = tx.schema().getConstraintByName(constraint.getName());
            assertThat(constraint.getConstraintType()).isEqualTo(ConstraintType.UNIQUENESS);
            assertThat(constraint.getLabel().name()).isEqualTo(label.name());
            assertThat(constraint.getPropertyKeys()).containsExactly(propertyKey);
            assertThat(constraint.getName()).isEqualTo("MyConstraint");
            tx.commit();
        }
    }

    @Test
    void shouldCreateNamedRelUniquenessConstraint() {
        // When
        ConstraintDefinition constraint = createRelUniquenessConstraint("MyConstraint", relType, propertyKey);

        // Then
        try (Transaction tx = db.beginTx()) {
            constraint = tx.schema().getConstraintByName(constraint.getName());
            assertThat(constraint.getConstraintType()).isEqualTo(ConstraintType.RELATIONSHIP_UNIQUENESS);
            assertThat(constraint.getRelationshipType().name()).isEqualTo(relType.name());
            assertThat(constraint.getPropertyKeys()).containsExactly(propertyKey);
            assertThat(constraint.getName()).isEqualTo("MyConstraint");
            tx.commit();
        }
    }

    @Test
    void shouldCreateUniquenessConstraintWithMultipleProperties() {
        // WHEN
        ConstraintDefinition constraint = createUniquenessConstraint(label, propertyKey, secondPropertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            constraint = tx.schema().getConstraintByName(constraint.getName());
            assertThat(constraint.getConstraintType()).isEqualTo(ConstraintType.UNIQUENESS);
            assertThat(constraint.getLabel().name()).isEqualTo(label.name());
            assertThat(constraint.getPropertyKeys()).containsExactly(propertyKey, secondPropertyKey);
            assertThat(constraint.getName()).isEqualTo("constraint_860007cd");
            tx.commit();
        }
    }

    @Test
    void shouldCreateRelUniquenessConstraintWithMultipleProperties() {
        // WHEN
        ConstraintDefinition constraint = createRelUniquenessConstraint(relType, propertyKey, secondPropertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            constraint = tx.schema().getConstraintByName(constraint.getName());

            assertThat(constraint.getConstraintType()).isEqualTo(ConstraintType.RELATIONSHIP_UNIQUENESS);
            assertThat(constraint.getRelationshipType().name()).isEqualTo(relType.name());
            assertThat(constraint.getPropertyKeys()).containsExactly(propertyKey, secondPropertyKey);
            assertThat(constraint.getName()).isEqualTo("constraint_ba789ec");

            tx.commit();
        }
    }

    @Test
    void shouldCreateNamedUniquenessConstraintWithMultipleProperties() {
        // WHEN
        ConstraintDefinition constraint =
                createUniquenessConstraint("MyConstraint", label, propertyKey, secondPropertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            constraint = tx.schema().getConstraintByName(constraint.getName());

            assertThat(constraint.getConstraintType()).isEqualTo(ConstraintType.UNIQUENESS);
            assertThat(constraint.getLabel().name()).isEqualTo(label.name());
            assertThat(constraint.getPropertyKeys()).containsExactly(propertyKey, secondPropertyKey);
            assertThat(constraint.getName()).isEqualTo("MyConstraint");
            tx.commit();
        }
    }

    @Test
    void shouldCreateNamedRelUniquenessConstraintWithMultipleProperties() {
        // WHEN
        ConstraintDefinition constraint =
                createRelUniquenessConstraint("MyConstraint", relType, propertyKey, secondPropertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            constraint = tx.schema().getConstraintByName(constraint.getName());

            assertThat(constraint.getConstraintType()).isEqualTo(ConstraintType.RELATIONSHIP_UNIQUENESS);
            assertThat(constraint.getRelationshipType().name()).isEqualTo(relType.name());
            assertThat(constraint.getPropertyKeys()).containsExactly(propertyKey, secondPropertyKey);
            assertThat(constraint.getName()).isEqualTo("MyConstraint");
            tx.commit();
        }
    }

    @Test
    void shouldGetConstraintByName() {
        ConstraintDefinition expectedConstraint = createUniquenessConstraint("MyConstraint", label, propertyKey);

        try (Transaction tx = db.beginTx()) {
            ConstraintDefinition actualConstraint = tx.schema().getConstraintByName("MyConstraint");
            assertThat(actualConstraint).isEqualTo(expectedConstraint);
            tx.commit();
        }
    }

    @Test
    void shouldListAddedConstraintsByLabel() {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint(label, propertyKey);
        createUniquenessConstraint(otherLabel, propertyKey);

        // WHEN THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(getConstraints(tx, label)).containsOnly(constraint1);
        }
    }

    private static Iterable<ConstraintDefinition> getConstraints(Transaction tx, Label label) {
        return tx.schema().getConstraints(label);
    }

    @Test
    void shouldListAddedConstraints() {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint(label, propertyKey);
        ConstraintDefinition constraint2 = createUniquenessConstraint(otherLabel, propertyKey);

        // WHEN THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(tx.schema().getConstraints()).containsOnly(constraint1, constraint2);
        }
    }

    @Test
    void shouldDropUniquenessConstraint() {
        // GIVEN
        ConstraintDefinition constraint = createUniquenessConstraint(label, propertyKey);

        // WHEN
        dropConstraint(db, constraint);

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(getConstraints(tx, label)).isEmpty();
        }
    }

    @Test
    void addingConstraintWhenIndexAlreadyExistsGivesNiceError() {
        // GIVEN
        createIndex(db, label, propertyKey);

        // WHEN
        assertThatThrownBy(() -> createUniquenessConstraint(label, propertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContainingAll(
                        "There already exists an index",
                        ":MY_LABEL",
                        "my_property_key",
                        "cannot be created until the index has been dropped");
    }

    @Test
    void addingUniquenessConstraintWhenDuplicateDataExistsGivesNiceError() {
        // GIVEN
        try (Transaction transaction = db.beginTx()) {
            transaction.createNode(label).setProperty(propertyKey, "value1");
            transaction.createNode(label).setProperty(propertyKey, "value1");
            transaction.commit();
        }

        // WHEN
        assertThatThrownBy(() -> createUniquenessConstraint(label, propertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContainingAll(
                        "Unable to create Constraint",
                        "name='constraint_d3208c60'",
                        "type='UNIQUENESS'",
                        "schema=(:MY_LABEL {my_property_key})",
                        "Note that only the first found violation is shown.");
    }

    @Test
    void addedUncommittedIndexesShouldBeVisibleWithinTheTransaction() {
        // GIVEN
        IndexDefinition indexA = createIndex(db, label, "a");
        createUniquenessConstraint(label, "b");

        // WHEN
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getIndexes(label))).isEqualTo(2L);
            IndexDefinition indexC = tx.schema().indexFor(label).on("c").create();
            // THEN
            assertThat(count(tx.schema().getIndexes(label))).isEqualTo(3L);
            assertThat(getIndexState(tx, indexA)).isEqualTo(ONLINE);
            assertThat(getIndexState(tx, indexC)).isEqualTo(POPULATING);
            assertThat(tx.schema().getIndexPopulationProgress(indexA).getCompletedPercentage())
                    .isGreaterThan(0f);
            assertThat(tx.schema().getIndexPopulationProgress(indexC).getCompletedPercentage())
                    .isGreaterThanOrEqualTo(0f);
        }
    }

    @Test
    void indexNamesMustBeUnique() {
        createIndex(db, "MyIndex", label, propertyKey);

        assertThatThrownBy(() -> createIndex(db, "MyIndex", label, secondPropertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContaining("MyIndex");
    }

    @Test
    void indexNamesMustBeUniqueEvenWhenGenerated() {
        IndexDefinition index = createIndex(db, label, propertyKey); // Index with generated name.

        assertThatThrownBy(() -> createIndex(db, index.getName(), otherLabel, secondPropertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContaining(index.getName());
    }

    @Test
    void indexNamesMustBeUniqueEvenWhenGenerated2() {
        IndexDefinition index = createIndex(db, "index_1efc11af", otherLabel, secondPropertyKey);

        assertThatThrownBy(() -> createIndex(db, label, propertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContaining(index.getName());
    }

    @Test
    void constraintNamesMustBeUnique() {
        createUniquenessConstraint("MyConstraint", label, propertyKey);

        assertThatThrownBy(() -> createUniquenessConstraint("MyConstraint", label, secondPropertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContaining("MyConstraint");
    }

    @Test
    void cannotCreateConstraintWithSameNameAsExistingIndex() {
        createIndex(db, "MySchema", label, propertyKey);

        assertThatThrownBy(() -> createUniquenessConstraint("MySchema", label, secondPropertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContaining("MySchema");
    }

    @Test
    void cannotCreateIndexWithSameNameAsExistingIndexWithGeneratedName() {
        IndexDefinition index = createIndex(db, label, propertyKey); // Index with generated name.

        assertThatThrownBy(() -> createIndex(db, index.getName(), otherLabel, secondPropertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContaining(index.getName());
    }

    @Test
    void cannotCreateConstraintWithSameNameAsExistingIndexWithGeneratedName() {
        IndexDefinition index = createIndex(db, label, propertyKey); // Index with generated name.
        assertThatThrownBy(() -> createUniquenessConstraint(index.getName(), otherLabel, secondPropertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContaining(index.getName());
    }

    @Test
    void cannotCreateIndexWithSameNameAsExistingConstraint() {
        createUniquenessConstraint("MySchema", label, propertyKey);

        assertThatThrownBy(() -> createIndex(db, "MySchema", label, secondPropertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContaining("MySchema");
    }

    @Test
    void cannotCreateIndexWithSameNameAsExistingConstraintWithGeneratedName() {
        ConstraintDefinition constraint = createUniquenessConstraint(label, propertyKey);

        assertThatThrownBy(() -> createIndex(db, constraint.getName(), label, secondPropertyKey))
                .isInstanceOf(ConstraintViolationException.class)
                .hasMessageContaining(constraint.getName());
    }

    @Test
    void uniquenessConstraintIndexesMustBeNamedAfterTheirConstraints() {
        createUniquenessConstraint("MySchema", label, propertyKey);
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "MySchema");
            assertThat(index.isConstraintIndex()).isTrue();
            assertThat(index.isNodeIndex()).isTrue();
            assertThat(index.getName()).isEqualTo("MySchema");
            tx.commit();
        }
    }

    @Test
    void indexNamesInTransactionStateMustBeUnique() {
        try (Transaction tx = db.beginTx()) {
            final String indexName = "MyIndex";
            tx.schema().indexFor(label).on(propertyKey).withName(indexName).create();
            IndexCreator indexCreator =
                    tx.schema().indexFor(otherLabel).on(secondPropertyKey).withName(indexName);

            assertThatThrownBy(indexCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessage(alreadyExistsIndexMessage(indexName));

            tx.commit();
        }
    }

    @Test
    void indexNamesInTransactionStateMustBeUniqueEvenWhenGenerated() {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema().indexFor(label).on(propertyKey).create();
            IndexCreator indexCreator =
                    tx.schema().indexFor(otherLabel).on(secondPropertyKey).withName(index.getName());

            assertThatThrownBy(indexCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessage(alreadyExistsIndexMessage(index.getName()));
            tx.commit();
        }
    }

    @Test
    void indexNamesInTransactionStateMustBeUniqueEvenWhenGenerated2() {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema()
                    .indexFor(otherLabel)
                    .on(secondPropertyKey)
                    .withName("index_1efc11af")
                    .create();
            IndexCreator indexCreator = tx.schema().indexFor(label).on(propertyKey);

            assertThatThrownBy(indexCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessage(alreadyExistsIndexMessage(index.getName()));
            tx.commit();
        }
    }

    @Test
    void constraintNamesInTransactionStateMustBeUnique() {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName("MyConstraint")
                    .create();
            ConstraintCreator constraintCreator = tx.schema()
                    .constraintFor(otherLabel)
                    .assertPropertyIsUnique(secondPropertyKey)
                    .withName("MyConstraint");

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessage(thereAlreadyExistsConstraintMessage("MyConstraint"));
            tx.commit();
        }
    }

    @Test
    void constraintNamesInTransactionStateMustBeUniqueEvenWhenGenerated() {
        try (Transaction tx = db.beginTx()) {
            ConstraintDefinition constraint = tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .create();
            ConstraintCreator constraintCreator = tx.schema()
                    .constraintFor(otherLabel)
                    .assertPropertyIsUnique(secondPropertyKey)
                    .withName(constraint.getName());

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining(constraint.getName());
            tx.commit();
        }
    }

    @Test
    void constraintNamesInTransactionStateMustBeUniqueEvenWhenGenerated2() {
        try (Transaction tx = db.beginTx()) {
            ConstraintDefinition constraint = tx.schema()
                    .constraintFor(otherLabel)
                    .assertPropertyIsUnique(secondPropertyKey)
                    .withName("constraint_d3208c60")
                    .create();
            ConstraintCreator constraintCreator =
                    tx.schema().constraintFor(label).assertPropertyIsUnique(propertyKey);

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining(thereAlreadyExistsConstraintMessage(constraint.getName()));
            tx.commit();
        }
    }

    @Test
    void constraintAndIndexNamesInTransactionStateMustBeUnique() {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName("MySchema")
                    .create();
            IndexCreator indexCreator =
                    tx.schema().indexFor(otherLabel).on(secondPropertyKey).withName("MySchema");
            assertThatThrownBy(indexCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("MySchema");
            tx.commit();
        }
    }

    @Test
    void indexAndConstraintNamesInTransactionStateMustBeUnique() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(label).on(propertyKey).withName("MySchema").create();
            ConstraintCreator constraintCreator = tx.schema()
                    .constraintFor(otherLabel)
                    .assertPropertyIsUnique(secondPropertyKey)
                    .withName("MySchema");

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("MySchema");
            tx.commit();
        }
    }

    @Test
    void nodeKeyConstraintsMustNotBeAvailableInCommunityEdition() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator =
                    tx.schema().constraintFor(label).assertPropertyIsNodeKey(propertyKey);

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Enterprise Edition");
            tx.commit();
        }
    }

    @Test
    void relationshipKeyConstraintsMustNotBeAvailableInCommunityEdition() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator =
                    tx.schema().constraintFor(relType).assertPropertyIsRelationshipKey(propertyKey);

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Enterprise Edition");
            tx.commit();
        }
    }

    @Test
    void propertyExistenceConstraintsMustNotBeAvailableInCommunityEdition() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator =
                    tx.schema().constraintFor(label).assertPropertyExists(propertyKey);

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Enterprise Edition");
            tx.commit();
        }
    }

    @Test
    void propertyExistenceConstraintsOnRelationshipMustNotBeAvailableInCommunityEdition() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator =
                    tx.schema().constraintFor(relType).assertPropertyExists(propertyKey);

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Enterprise Edition");
            tx.commit();
        }
    }

    @Test
    void nodePropertyTypeConstraintsMustNotBeAvailableInCommunityEdition() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator =
                    tx.schema().constraintFor(label).assertPropertyHasType(propertyKey, PropertyType.DATE);

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Enterprise Edition");
            tx.commit();
        }
    }

    @Test
    void relationshipPropertyTypeConstraintsMustNotBeAvailableInCommunityEdition() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator =
                    tx.schema().constraintFor(relType).assertPropertyHasType(propertyKey, PropertyType.BOOLEAN);

            assertThatThrownBy(constraintCreator::create)
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Enterprise Edition");
            tx.commit();
        }
    }

    @Test
    void indexNamesCanContainBackTicks() {
        final String BACK_TICK_NAME = "`a`b``";
        assertThatCode(() -> {
                    try (Transaction tx = db.beginTx()) {
                        IndexCreator indexCreator = tx.schema()
                                .indexFor(label)
                                .withName(BACK_TICK_NAME)
                                .on(propertyKey);
                        indexCreator.create();
                        tx.commit();
                    }
                })
                .doesNotThrowAnyException();

        try (Transaction tx = db.beginTx()) {
            assertThat(tx.schema().getIndexes())
                    .map(IndexDefinition::getName)
                    .as("index exists")
                    .contains(BACK_TICK_NAME);
            tx.commit();
        }
    }

    @Test
    void indexTokensCanContainBackTicks() {
        final String NAME = "abc";
        assertThatCode(() -> {
                    try (Transaction tx = db.beginTx()) {
                        IndexCreator indexCreator = tx.schema()
                                .indexFor(labelWithBackticks)
                                .withName(NAME)
                                .on(propertyKeyWithBackticks);
                        indexCreator.create();
                        tx.commit();
                    }
                })
                .doesNotThrowAnyException();

        try (Transaction tx = db.beginTx()) {
            var index = tx.schema().getIndexByName(NAME);
            assertThat(index.getName()).isEqualTo(NAME);
            assertThat(index.getLabels()).containsExactly(labelWithBackticks);
            assertThat(index.getPropertyKeys()).containsExactly(propertyKeyWithBackticks);
            assertThat(tx.schema().getConstraints()).isEmpty();
            tx.commit();
        }
    }

    @Test
    void constraintNamesCanContainBackTicks() {
        final String BACK_TICK_NAME = "`a`b``";
        assertThatCode(() -> {
                    try (Transaction tx = db.beginTx()) {
                        ConstraintCreator constraintCreator = tx.schema()
                                .constraintFor(label)
                                .withName(BACK_TICK_NAME)
                                .assertPropertyIsUnique(propertyKey);
                        constraintCreator.create();
                        tx.commit();
                    }
                })
                .doesNotThrowAnyException();
        try (Transaction tx = db.beginTx()) {
            assertThat(tx.schema().getIndexes())
                    .map(IndexDefinition::getName)
                    .as("index exists")
                    .contains(BACK_TICK_NAME);

            assertThat(tx.schema().getConstraints())
                    .map(ConstraintDefinition::getName)
                    .as("constraint exists")
                    .contains(BACK_TICK_NAME);
            tx.commit();
        }
    }

    @Test
    void constraintTokensCanContainBackTicks() {
        String name = "abc";

        assertThatCode(() -> {
                    try (Transaction tx = db.beginTx()) {
                        ConstraintCreator constraintCreator = tx.schema()
                                .constraintFor(labelWithBackticks)
                                .withName(name)
                                .assertPropertyIsUnique(propertyKeyWithBackticks);
                        constraintCreator.create();
                        tx.commit();
                    }
                })
                .doesNotThrowAnyException();

        try (Transaction tx = db.beginTx()) {
            final IndexDefinition index = tx.schema().getIndexByName(name);
            assertThat(index.getName()).isEqualTo(name);
            assertThat(index.getLabels()).containsExactly(labelWithBackticks);
            assertThat(index.getPropertyKeys()).containsExactly(propertyKeyWithBackticks);

            final ConstraintDefinition constraint = tx.schema().getConstraintByName(name);
            assertThat(constraint.getName()).isEqualTo(name);
            assertThat(constraint.getLabel()).isEqualTo(labelWithBackticks);
            assertThat(constraint.getPropertyKeys()).containsExactly(propertyKeyWithBackticks);
            tx.commit();
        }
    }

    @Test
    void mustCreateFullTextIndexBySettingIndexType() {
        try (Transaction tx = db.beginTx()) {
            IndexCreator indexCreator =
                    tx.schema().indexFor(label).on(propertyKey).withIndexType(IndexType.FULLTEXT);
            IndexDefinition definition = indexCreator.create();
            assertThat(definition.getIndexType()).isEqualTo(IndexType.FULLTEXT);

            IndexProviderDescriptor provider =
                    ((IndexDefinitionImpl) definition).getIndexReference().getIndexProvider();
            assertThat(AllIndexProviderDescriptors.FULLTEXT_DESCRIPTOR).isEqualTo(provider);
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToGetIndexConfig() {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema()
                    .indexFor(label)
                    .on(propertyKey)
                    .withIndexType(POINT)
                    .withName("my_index")
                    .create();
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat(config).isNotNull().containsKey(IndexSettingImpl.SPATIAL_CARTESIAN_MIN);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "my_index");
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat(config).isNotNull().containsKey(IndexSettingImpl.SPATIAL_CARTESIAN_MAX);
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToGetFullTextIndexConfig() {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema()
                    .indexFor(label)
                    .withName("my_index")
                    .on(propertyKey)
                    .withIndexType(IndexType.FULLTEXT)
                    .create();
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat(config).isNotNull().containsKey(IndexSettingImpl.FULLTEXT_ANALYZER);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "my_index");
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat(config).isNotNull().containsKey(IndexSettingImpl.FULLTEXT_ANALYZER);
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToSetFullTextIndexConfig() {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema()
                    .indexFor(label)
                    .withName("my_index")
                    .on(propertyKey)
                    .withIndexType(IndexType.FULLTEXT)
                    .withIndexConfiguration(Map.of(
                            IndexSettingImpl.FULLTEXT_ANALYZER,
                            "swedish",
                            IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT,
                            true))
                    .create();
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat(config.get(IndexSettingImpl.FULLTEXT_ANALYZER)).isEqualTo("swedish");
            assertThat(config.get(IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT))
                    .isEqualTo(true);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "my_index");
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat(config.get(IndexSettingImpl.FULLTEXT_ANALYZER)).isEqualTo("swedish");
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToSetPointIndexConfig() {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema()
                    .indexFor(label)
                    .withName("my_index")
                    .on(propertyKey)
                    .withIndexType(POINT)
                    .withIndexConfiguration(Map.of(
                            IndexSettingImpl.SPATIAL_CARTESIAN_MAX,
                            TEST_ARRAY1,
                            IndexSettingImpl.SPATIAL_WGS84_MIN,
                            TEST_ARRAY2))
                    .create();
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat((double[]) config.get(IndexSettingImpl.SPATIAL_CARTESIAN_MAX))
                    .containsExactly(TEST_ARRAY1);
            assertThat((double[]) config.get(IndexSettingImpl.SPATIAL_WGS84_MIN))
                    .containsExactly(TEST_ARRAY2);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "my_index");
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat((double[]) config.get(IndexSettingImpl.SPATIAL_CARTESIAN_MAX))
                    .containsExactly(TEST_ARRAY1);
            assertThat((double[]) config.get(IndexSettingImpl.SPATIAL_WGS84_MIN))
                    .containsExactly(TEST_ARRAY2);
            tx.commit();
        }
    }

    /**
     * This is the example used in {@link IndexSettingImpl}.
     */
    @Test
    void indexConfigurationExample() {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(Label.label("Email"))
                    .on("from")
                    .on("to")
                    .on("cc")
                    .on("bcc")
                    .withName("email-addresses")
                    .withIndexType(IndexType.FULLTEXT)
                    .withIndexConfiguration(Map.of(IndexSetting.fulltext_Analyzer(), "email"))
                    .create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "email-addresses");
            assertThat(index.getPropertyKeys()).contains("from", "to", "cc", "bcc");
            assertThat(index.getIndexConfiguration().get(IndexSetting.fulltext_Analyzer()))
                    .isEqualTo("email");
            tx.commit();
        }
    }

    @Test
    void indexSettingValuesMustHaveCorrectType() {
        try (Transaction tx = db.beginTx()) {
            IndexCreator indexCreator =
                    tx.schema().indexFor(label).withName("my_index").on(propertyKey);
            assertThatThrownBy(() -> indexCreator
                            .withIndexType(IndexType.FULLTEXT)
                            .withIndexConfiguration(Map.of(IndexSettingImpl.FULLTEXT_ANALYZER, 1))
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> indexCreator
                            .withIndexType(IndexType.FULLTEXT)
                            .withIndexConfiguration(Map.of(IndexSettingImpl.FULLTEXT_ANALYZER, true))
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> indexCreator
                            .withIndexType(IndexType.FULLTEXT)
                            .withIndexConfiguration(Map.of(IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT, "true"))
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> indexCreator
                            .withIndexType(IndexType.FULLTEXT)
                            .withIndexConfiguration(Map.of(IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT, 1))
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> indexCreator
                            .withIndexConfiguration(Map.of(IndexSettingImpl.SPATIAL_CARTESIAN_MAX, "1"))
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> indexCreator
                            .withIndexConfiguration(Map.of(IndexSettingImpl.SPATIAL_CARTESIAN_MAX, 1))
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> indexCreator
                            .withIndexConfiguration(Map.of(IndexSettingImpl.SPATIAL_CARTESIAN_MAX, 1.0))
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);

            tx.commit();
        }
    }

    @Test
    void indexCreatorThrowsOnUnsupportedIndexType() {
        try (var tx = db.beginTx()) {
            IndexCreator indexCreator =
                    tx.schema().indexFor(label).withName("my_index").on(propertyKey);

            assertThatThrownBy(
                            () -> indexCreator.withIndexType(IndexType.LOOKUP).create())
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Index type LOOKUP is not supported for property indexes.");
        }
    }

    @Test
    void tokenIndexCreatorThrowsOnProperty() {
        try (var tx = db.beginTx()) {
            IndexCreator indexCreator = tx.schema().indexFor(AnyTokens.ANY_LABELS);

            assertThatThrownBy(() -> indexCreator.on("property"))
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("LOOKUP indexes doesn't support inclusion of property keys.");
        }
    }

    @Test
    void tokenIndexCreatorThrowsOnUnsupportedIndexTypes() {
        try (var tx = db.beginTx()) {
            IndexCreator indexCreator = tx.schema().indexFor(AnyTokens.ANY_LABELS);

            assertThatThrownBy(() -> indexCreator.withIndexType(IndexType.RANGE))
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Only LOOKUP index type supported for token indexes.");
            assertThatThrownBy(() -> indexCreator.withIndexType(IndexType.FULLTEXT))
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("Only LOOKUP index type supported for token indexes.");
        }
    }

    @Test
    void indexSettingsWithNonsensicalValuesMustBeRejected() {
        try (Transaction tx = db.beginTx()) {
            Exception e;
            IndexCreator indexCreator =
                    tx.schema().indexFor(label).withName("my_index").on(propertyKey);
            assertThatThrownBy(() -> indexCreator
                            .withIndexType(IndexType.FULLTEXT)
                            .withIndexConfiguration(
                                    Map.of(IndexSettingImpl.FULLTEXT_ANALYZER, "analyzer that does not exist"))
                            .create())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("'analyzer that does not exist'");

            assertThatThrownBy(() -> indexCreator
                            .withIndexType(POINT)
                            .withIndexConfiguration(
                                    Map.of(IndexSettingImpl.SPATIAL_CARTESIAN_MAX, new double[] {100.0, 10.0, 1.0}))
                            .create())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid spatial index settings");

            tx.commit();
        }
    }

    @Test
    void creatingFullTextIndexOnMultipleLabelsMustBePossible() {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema()
                    .indexFor(label, otherLabel)
                    .on(propertyKey)
                    .withIndexType(IndexType.FULLTEXT)
                    .withName("index")
                    .create();
            assertThat(index.getLabels()).contains(label, otherLabel);
            assertThat(index.isMultiTokenIndex()).isTrue();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "index");
            List<String> labelNames = new ArrayList<>();
            index.getLabels().forEach(label -> labelNames.add(label.name()));
            assertThat(labelNames).contains(label.name(), otherLabel.name());
            assertThat(index.isMultiTokenIndex()).isTrue();
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingRangeIndexWithZeroLabels() {
        try (Transaction tx = db.beginTx()) {
            assertThatThrownBy(() ->
                            tx.schema().indexFor(new Label[0]).on(propertyKey).create())
                    .isInstanceOf(IllegalArgumentException.class);
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingRangeIndexWithMoreThanOneLabel() {
        try (Transaction tx = db.beginTx()) {
            assertThatThrownBy(() -> tx.schema()
                            .indexFor(label, otherLabel)
                            .on(propertyKey)
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingFullTextIndexWithZeroLabels() {
        try (Transaction tx = db.beginTx()) {
            assertThatThrownBy(() -> tx.schema()
                            .indexFor(new Label[0])
                            .on(propertyKey)
                            .withIndexType(IndexType.FULLTEXT)
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);
            tx.commit();
        }
    }

    @Test
    void creatingFullTextIndexOnRelationshipTypeMustBePossible() {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema()
                    .indexFor(relType)
                    .on(propertyKey)
                    .withIndexType(IndexType.FULLTEXT)
                    .withName("index")
                    .create();
            assertThat(index.isRelationshipIndex()).isTrue();
            assertThat(index.getRelationshipTypes()).contains(relType);
            assertThat(index.getIndexType()).isEqualTo(IndexType.FULLTEXT);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "index");
            assertThat(index.isRelationshipIndex()).isTrue();
            assertThat(index.getRelationshipTypes()).contains(relType);
            assertThat(index.getIndexType()).isEqualTo(IndexType.FULLTEXT);
            tx.commit();
        }
    }

    @Test
    void creatingMultiTokenFullTextIndexOnRelationshipTypesMustBePossible() {
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema()
                    .indexFor(relType, otherRelType)
                    .on(propertyKey)
                    .withIndexType(IndexType.FULLTEXT)
                    .withName("index")
                    .create();
            assertThat(index.isRelationshipIndex()).isTrue();
            assertThat(index.getRelationshipTypes()).contains(relType, otherRelType);
            assertThat(index.getIndexType()).isEqualTo(IndexType.FULLTEXT);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "index");
            assertThat(index.isRelationshipIndex()).isTrue();
            assertThat(index.getRelationshipTypes()).contains(relType, otherRelType);
            assertThat(index.getIndexType()).isEqualTo(IndexType.FULLTEXT);
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingFullTextIndexOnZeroRelationshipTypes() {
        try (Transaction tx = db.beginTx()) {
            assertThatThrownBy(() -> tx.schema()
                            .indexFor(new RelationshipType[0])
                            .on(propertyKey)
                            .withIndexType(IndexType.FULLTEXT)
                            .create())
                    .isInstanceOf(IllegalArgumentException.class);
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingRangeIndexOnZeroRelationshipTypes() {
        try (Transaction tx = db.beginTx()) {

            assertThatThrownBy(() -> tx.schema()
                            .indexFor(new RelationshipType[0])
                            .on(propertyKey)
                            .create())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "RANGE indexes can only be created with exactly one relationship type, but got no relationship types.");
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingRangeIndexOnMultipleRelationshipTypes() {
        try (Transaction tx = db.beginTx()) {
            assertThatThrownBy(() -> tx.schema()
                            .indexFor(relType, otherRelType)
                            .on(propertyKey)
                            .create())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "RANGE indexes can only be created with exactly one relationship type, but got 2 relationship types.");
            tx.commit();
        }
    }

    @Test
    void uniquenessConstraintIndexesAreRangeIndexTypeByDefault() {
        String name;
        try (Transaction tx = db.beginTx()) {
            ConstraintDefinition constraint = tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .create();
            name = constraint.getName();
            IndexDefinition index = getIndex(tx, name);
            assertThat(index.getIndexType()).isEqualTo(RANGE);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, name);
            assertThat(index.getIndexType()).isEqualTo(RANGE);
        }
    }

    @Test
    void creatingUniquenessConstraintWithFullTextIndexTypeMustThrow() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator = tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withIndexType(FULLTEXT);
            assertThatThrownBy(constraintCreator::create).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void creatingNodePropertyExistenceConstraintMustThrowWhenGivenIndexType() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator = tx.schema()
                    .constraintFor(label)
                    .assertPropertyExists(propertyKey)
                    .withIndexType(RANGE);
            assertThatThrownBy(constraintCreator::create).isInstanceOf(IllegalArgumentException.class);
            tx.commit();
        }
    }

    @Test
    void creatingRelationshipPropertyExistenceConstraintsMustThrowWhenGivenIndexType() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator = tx.schema()
                    .constraintFor(relType)
                    .assertPropertyExists(propertyKey)
                    .withIndexType(RANGE);
            assertThatThrownBy(constraintCreator::create).isInstanceOf(IllegalArgumentException.class);
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToSpecifyIndexConfigurationForUniquenessConstraint() {
        try (Transaction tx = db.beginTx()) {
            ConstraintDefinition constraint = tx.schema()
                    .constraintFor(label)
                    .withName("my constraint")
                    .assertPropertyIsUnique(propertyKey)
                    .withIndexConfiguration(Map.of(
                            IndexSettingImpl.SPATIAL_CARTESIAN_MAX,
                            TEST_ARRAY1,
                            IndexSettingImpl.SPATIAL_WGS84_MIN,
                            TEST_ARRAY2))
                    .create();
            IndexDefinition index = getIndex(tx, constraint.getName());
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat((double[]) config.get(IndexSettingImpl.SPATIAL_CARTESIAN_MAX))
                    .isEqualTo(TEST_ARRAY1);
            assertThat((double[]) config.get(IndexSettingImpl.SPATIAL_WGS84_MIN))
                    .isEqualTo(TEST_ARRAY2);
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = getIndex(tx, "my constraint");
            Map<IndexSetting, Object> config = index.getIndexConfiguration();
            assertThat((double[]) config.get(IndexSettingImpl.SPATIAL_CARTESIAN_MAX))
                    .isEqualTo(TEST_ARRAY1);
            assertThat((double[]) config.get(IndexSettingImpl.SPATIAL_WGS84_MIN))
                    .isEqualTo(TEST_ARRAY2);
            tx.commit();
        }
    }

    @Test
    void creatingNodePropertyExistenceConstraintMustThrowWhenGivenIndexConfiguration() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator = tx.schema()
                    .constraintFor(label)
                    .withIndexConfiguration(Map.of())
                    .assertPropertyExists(propertyKey);
            assertThatThrownBy(constraintCreator::create).isInstanceOf(IllegalArgumentException.class);
            tx.commit();
        }
    }

    @Test
    void creatingRelationshipPropertyExistenceConstraintMustThrowWhenGivenIndexConfiguration() {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator = tx.schema()
                    .constraintFor(relType)
                    .withIndexConfiguration(Map.of())
                    .assertPropertyExists(propertyKey);
            assertThatThrownBy(constraintCreator::create).isInstanceOf(IllegalArgumentException.class);
            tx.commit();
        }
    }

    @Test
    void creatingRangeRelationshipIndex() {
        IndexDefinition index = createIndex(db, relType, propertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, relType)).containsOnly(index);
        }
    }

    @Test
    void creatingCompositeRangeRelationshipIndex() {
        IndexDefinition index = createIndex(db, relType, propertyKey, secondPropertyKey);

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexes(tx, relType)).containsOnly(index);
        }
    }

    @Test
    void mustBePossibleToGetFulltextIndexesBasedOnRelationshipType() {
        IndexDefinition index1;
        IndexDefinition index2;
        try (Transaction tx = db.beginTx()) {
            index1 = tx.schema()
                    .indexFor(relType)
                    .on(propertyKey)
                    .withIndexType(IndexType.FULLTEXT)
                    .create();
            index2 = tx.schema()
                    .indexFor(relType, otherRelType)
                    .on(propertyKey)
                    .withIndexType(IndexType.FULLTEXT)
                    .create();
            tx.commit();
        }
        waitForIndex(db, index1);
        waitForIndex(db, index2);

        try (Transaction tx = db.beginTx()) {
            assertThat(tx.schema().getIndexes(relType)).containsExactly(index1, index2);
            assertThat(tx.schema().getIndexes(otherRelType)).containsOnly(index2);
            assertThat(tx.schema().getIndexes(thirdRelType)).isEmpty();
            tx.commit();
        }
    }

    @Test
    void commitTwoIndexes() {
        // Given
        IndexDefinition indexA;
        IndexDefinition indexB;
        try (Transaction tx = db.beginTx()) {
            indexA = tx.schema().indexFor(label).on(propertyKey).withName(nameA).create();
            indexB = tx.schema()
                    .indexFor(otherLabel)
                    .on(propertyKey)
                    .withName(nameB)
                    .create();
            tx.commit();
        }
        waitForIndexes(db);

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(getIndexState(tx, indexA)).isEqualTo(ONLINE);
            assertThat(getIndexState(tx, indexB)).isEqualTo(ONLINE);
            tx.commit();
        }
    }

    @Test
    void rollbackTwoIndexes() {
        // Given
        IndexDefinition indexA;
        IndexDefinition indexB;
        try (Transaction tx = db.beginTx()) {
            indexA = tx.schema().indexFor(label).on(propertyKey).withName(nameA).create();
            indexB = tx.schema()
                    .indexFor(otherLabel)
                    .on(propertyKey)
                    .withName(nameB)
                    .create();
            tx.rollback();
        }
        waitForIndexes(db);

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getIndexes())).isEqualTo(0);
            assertThatThrownBy(() -> getIndexState(tx, indexA)).isInstanceOf(NotFoundException.class);
            assertThatThrownBy(() -> getIndexState(tx, indexB)).isInstanceOf(NotFoundException.class);
            tx.commit();
        }
    }

    @Test
    void implicitRollbackTxOnConflictOnFirstIndexCreation() {
        // Given
        IndexDefinition oldIndexA = createIndex(db, nameA, label, propertyKey);
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema()
                                .indexFor(label)
                                .on(propertyKey)
                                .withName(nameA)
                                .create();
                        tx.schema()
                                .indexFor(otherLabel)
                                .on(propertyKey)
                                .withName(nameB)
                                .create();
                        tx.commit();
                    }
                })
                .isInstanceOf(ConstraintViolationException.class);
        waitForIndexes(db);

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getIndexes())).isEqualTo(1);
            assertThat(getIndexState(tx, oldIndexA)).isEqualTo(ONLINE);
            tx.commit();
        }
    }

    @Test
    void implicitRollbackTxOnConflictOnSecondIndexCreation() {
        // Given
        IndexDefinition oldIndexB = createIndex(db, nameB, otherLabel, propertyKey);
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema()
                                .indexFor(label)
                                .on(propertyKey)
                                .withName(nameA)
                                .create();
                        tx.schema()
                                .indexFor(otherLabel)
                                .on(propertyKey)
                                .withName(nameB)
                                .create();
                        tx.commit();
                    }
                })
                .isInstanceOf(ConstraintViolationException.class);
        waitForIndexes(db);

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getIndexes())).isEqualTo(1);
            assertThat(getIndexState(tx, oldIndexB)).isEqualTo(ONLINE);
            tx.commit();
        }
    }

    @Test
    void createAndDropNewIndexInSameTxIsNoOp() {
        IndexDefinition indexA;
        try (Transaction tx = db.beginTx()) {
            indexA = tx.schema().indexFor(label).on(propertyKey).withName(nameA).create();
            getIndex(tx, nameA).drop();
            tx.commit();
        }
        waitForIndexes(db);

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getIndexes())).isEqualTo(0);
            assertThatThrownBy(() -> getIndexState(tx, indexA)).isInstanceOf(NotFoundException.class);
            tx.commit();
        }
    }

    @Test
    void dropOldIndexAndCreateNewIdenticalCreatesNewIndex() {
        IndexDefinition oldIndexA = createIndex(db, nameA, label, propertyKey);
        IndexDefinition indexA;
        try (Transaction tx = db.beginTx()) {
            getIndex(tx, nameA).drop();
            indexA = tx.schema().indexFor(label).on(propertyKey).withName(nameA).create();
            tx.commit();
        }
        waitForIndexes(db);

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getIndexes())).isEqualTo(1);
            assertThatThrownBy(() -> getIndexState(tx, oldIndexA)).isInstanceOf(NotFoundException.class);
            assertThat(getIndexState(tx, indexA)).isEqualTo(ONLINE);
            tx.commit();
        }
    }

    @Test
    void indexPopulationFailureWillOnlyFailAffectedIndex() {
        // Given
        try (Transaction tx = db.beginTx()) {
            tx.createNode(label).setProperty(propertyKey, tooLargeString());
            tx.commit();
        }

        // When
        IndexDefinition indexA;
        IndexDefinition indexB;
        try (Transaction tx = db.beginTx()) {
            indexA = tx.schema().indexFor(label).on(propertyKey).withName(nameA).create();
            indexB = tx.schema()
                    .indexFor(otherLabel)
                    .on(propertyKey)
                    .withName(nameB)
                    .create();
            tx.commit();
        }

        // Then
        assertThatThrownBy(() -> waitForIndexes(db)).isInstanceOf(IllegalStateException.class);
        waitForIndex(db, indexB);
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getIndexes())).isEqualTo(2);
            assertThat(getIndexState(tx, indexA)).isEqualTo(FAILED);
            assertThat(getIndexState(tx, indexB)).isEqualTo(ONLINE);
            tx.commit();
        }
    }

    @Test
    void shouldNotUseFailedIndexToFindEntities() {
        String expectedNode;
        // Given
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(label);
            expectedNode = node.getElementId();
            node.setProperty(propertyKey, "somevalue");
            // Property that can't be indexed
            tx.createNode(label).setProperty(propertyKey, tooLargeString());
            tx.commit();
        }

        // When
        IndexDefinition indexA;
        try (Transaction tx = db.beginTx()) {
            indexA = tx.schema().indexFor(label).on(propertyKey).withName(nameA).create();
            tx.commit();
        }

        // Then the index ends up in failed state
        assertThatThrownBy(() -> waitForIndexes(db)).isInstanceOf(IllegalStateException.class);
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getIndexes())).isEqualTo(1);
            assertThat(getIndexState(tx, indexA)).isEqualTo(FAILED);
            tx.commit();
        }

        // but it should still be possible to find entities (without trying to use the index and getting exceptions)
        try (Transaction tx = db.beginTx();
                ResourceIterator<Node> nodes = tx.findNodes(label, propertyKey, "somevalue")) {
            assertThat(nodes).hasNext();
            assertThat(nodes.next().getElementId()).isEqualTo(expectedNode);
            assertThat(nodes).isExhausted();
        }
    }

    @Test
    void commitTwoConstraintsSameTx() {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(nameA)
                    .create();
            tx.schema()
                    .constraintFor(otherLabel)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(nameB)
                    .create();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getConstraints())).isEqualTo(2);
            assertThat(getIndexState(tx, nameA)).isEqualTo(ONLINE);
            assertThat(getIndexState(tx, nameB)).isEqualTo(ONLINE);
            tx.commit();
        }
    }

    @Test
    void rollbackTwoConstraintsSameTx() {
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(nameA)
                    .create();
            tx.schema()
                    .constraintFor(otherLabel)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(nameB)
                    .create();
            tx.rollback();
        }

        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getConstraints())).isEqualTo(0);
            assertThat(count(tx.schema().getIndexes())).isEqualTo(0);
            tx.commit();
        }
    }

    @Test
    void implicitRollbackIfIndexPopulationFailureOnFirstConstraint() {
        // Given
        try (Transaction tx = db.beginTx()) {
            tx.createNode(label).setProperty(propertyKey, tooLargeString());
            tx.commit();
        }
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema()
                                .constraintFor(label)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameA)
                                .create();
                        tx.schema()
                                .constraintFor(otherLabel)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameB)
                                .create();
                        tx.commit();
                    }
                })
                .isInstanceOf(ConstraintViolationException.class);

        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getConstraints())).isEqualTo(0);
            assertThat(count(tx.schema().getIndexes())).isEqualTo(0);
            tx.commit();
        }
    }

    @Test
    void implicitRollbackIfIndexPopulationFailureOnSecondConstraint() {
        // Given
        try (Transaction tx = db.beginTx()) {
            tx.createNode(otherLabel).setProperty(propertyKey, tooLargeString());
            tx.commit();
        }

        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema()
                                .constraintFor(label)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameA)
                                .create();
                        tx.schema()
                                .constraintFor(otherLabel)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameB)
                                .create();
                        tx.commit();
                    }
                })
                .isInstanceOf(ConstraintViolationException.class);

        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getConstraints())).isEqualTo(0);
            assertThat(count(tx.schema().getIndexes())).isEqualTo(0);
            tx.commit();
        }
    }

    @Test
    void implicitRollbackIfConstraintViolationOnFirstConstraint() {
        // Given
        try (Transaction tx = db.beginTx()) {
            tx.createNode(label).setProperty(propertyKey, "Non-unique string");
            tx.createNode(label).setProperty(propertyKey, "Non-unique string");
            tx.commit();
        }

        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema()
                                .constraintFor(label)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameA)
                                .create();
                        tx.schema()
                                .constraintFor(otherLabel)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameB)
                                .create();
                        tx.commit();
                    }
                })
                .isInstanceOf(ConstraintViolationException.class);

        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getConstraints())).isEqualTo(0);
            assertThat(count(tx.schema().getIndexes())).isEqualTo(0);
            tx.commit();
        }
    }

    @Test
    void implicitRollbackIfConstraintViolationOnSecondConstraint() {
        // Given
        try (Transaction tx = db.beginTx()) {
            tx.createNode(otherLabel).setProperty(propertyKey, "Non-unique string");
            tx.createNode(otherLabel).setProperty(propertyKey, "Non-unique string");
            tx.commit();
        }
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema()
                                .constraintFor(label)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameA)
                                .create();
                        tx.schema()
                                .constraintFor(otherLabel)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameB)
                                .create();
                        tx.commit();
                    }
                })
                .isInstanceOf(ConstraintViolationException.class);

        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getConstraints())).isEqualTo(0);
            assertThat(count(tx.schema().getIndexes())).isEqualTo(0);
            tx.commit();
        }
    }

    @Test
    void implicitRollbackIfFirstConstraintAlreadyExists() {
        // Given
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(nameA)
                    .create();
            tx.commit();
        }

        // When
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema()
                                .constraintFor(label)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameA)
                                .create();
                        tx.schema()
                                .constraintFor(otherLabel)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameB)
                                .create();
                        tx.commit();
                    }
                })
                .isInstanceOf(ConstraintViolationException.class);

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getConstraints())).isEqualTo(1);
            assertThat(count(tx.schema().getIndexes())).isEqualTo(1);
            tx.commit();
        }
    }

    @Test
    void implicitRollbackIfSecondConstraintAlreadyExists() {
        // Given
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .constraintFor(otherLabel)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(nameB)
                    .create();
            tx.commit();
        }

        // When
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.schema()
                                .constraintFor(label)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameA)
                                .create();
                        tx.schema()
                                .constraintFor(otherLabel)
                                .assertPropertyIsUnique(propertyKey)
                                .withName(nameB)
                                .create();
                        tx.commit();
                    }
                })
                .isInstanceOf(ConstraintViolationException.class);

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getConstraints())).isEqualTo(1);
            assertThat(count(tx.schema().getIndexes())).isEqualTo(1);
            tx.commit();
        }
    }

    @Test
    void createAndDropConstraintInSameTx() {
        // When
        try (Transaction tx = db.beginTx()) {
            ConstraintDefinition constraint = tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(nameA)
                    .create();
            constraint.drop();
            tx.commit();
        }

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(count(tx.schema().getConstraints())).isEqualTo(0);
            assertThat(count(tx.schema().getIndexes())).isEqualTo(0);
            tx.commit();
        }
    }

    @Test
    void dropUniquenessConstraintAndCreateSimilarUniquenessInSameTxMustThrow() {
        ConstraintCreateOperation initial = (schema, prop, name) -> schema.constraintFor(label)
                .assertPropertyIsUnique(prop)
                .withName(name)
                .create();
        ConstraintCreateOperation similar = (schema, prop, name) -> schema.constraintFor(label)
                .assertPropertyIsUnique(prop)
                .withName(name)
                .create();
        dropIndexBackedConstraintAndCreateSimilarInSameTxMustThrow(db, initial, similar);
    }

    @Test
    void dropUniquenessConstraintAndCreateDifferentUniquenessInSameTxMustSucceed() {
        ConstraintCreateOperation initial = (schema, prop, name) -> schema.constraintFor(label)
                .assertPropertyIsUnique(prop)
                .withName(name)
                .create();
        ConstraintCreateOperation similar = (schema, prop, name) -> schema.constraintFor(label)
                .assertPropertyIsUnique(prop)
                .withName(name)
                .create();
        dropIndexBackedConstraintAndCreateSlightlyDifferentInSameTxMustSucceed(db, initial, similar);
    }

    @Test
    void dropRelUniquenessConstraintAndCreateSimilarUniquenessInSameTxMustThrow() {
        ConstraintCreateOperation initial = (schema, prop, name) -> schema.constraintFor(relType)
                .assertPropertyIsUnique(prop)
                .withName(name)
                .create();
        ConstraintCreateOperation similar = (schema, prop, name) -> schema.constraintFor(relType)
                .assertPropertyIsUnique(prop)
                .withName(name)
                .create();
        dropIndexBackedConstraintAndCreateSimilarInSameTxMustThrow(db, initial, similar);
    }

    @Test
    void dropRelUniquenessConstraintAndCreateDifferentUniquenessInSameTxMustSucceed() {
        ConstraintCreateOperation initial = (schema, prop, name) -> schema.constraintFor(relType)
                .assertPropertyIsUnique(prop)
                .withName(name)
                .create();
        ConstraintCreateOperation similar = (schema, prop, name) -> schema.constraintFor(relType)
                .assertPropertyIsUnique(prop)
                .withName(name)
                .create();
        dropIndexBackedConstraintAndCreateSlightlyDifferentInSameTxMustSucceed(db, initial, similar);
    }

    @Test
    void terminatingConstraintTransactionShouldNotLeaveIndexBehind() throws InterruptedException, ExecutionException {
        AtomicReference<Path> indexDir = new AtomicReference<>();
        // Monitor that will stop in population so the transaction can be terminated while not holding the index lock.
        Barrier.Control midPopulation = new Barrier.Control();
        IndexMonitor.MonitorAdapter trappingMonitor = new IndexMonitor.MonitorAdapter() {
            @Override
            public void indexPopulationScanStarting(IndexDescriptor[] indexDescriptors) {
                for (IndexDescriptor indexDescriptor : indexDescriptors) {
                    if (indexDescriptor.getName().equals("hej")) {
                        // Also figure out the index's directory while we have all the necessary information
                        IndexDirectoryStructure indexDirectoryStructure = directoriesByProvider(
                                        ((GraphDatabaseAPI) db).databaseLayout().databaseDirectory())
                                .forProvider(indexDescriptor.getIndexProvider());
                        IndexFiles indexFiles = new IndexFiles(fs, indexDirectoryStructure, indexDescriptor.getId());
                        indexDir.set(indexFiles.getBase());

                        midPopulation.reached();
                    }
                }
            }
        };
        Monitors monitors = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Monitors.class);
        monitors.addMonitorListener(trappingMonitor);

        final Label x = Label.label("X");
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 10; i++) {
                tx.createNode(x).setProperty("x", "" + i);
            }
            tx.commit();
        }

        Future<Object> constraintCreation = otherThread.execute(() -> {
            assertThatThrownBy(() -> {
                        try (Transaction tx = db.beginTx()) {
                            ((TransactionImpl) tx).setMetaData(Map.of("terminateMe", true));
                            tx.schema()
                                    .constraintFor(x)
                                    .assertPropertyIsUnique("x")
                                    .withName("hej")
                                    .create();
                            tx.commit();
                        }
                    })
                    .isInstanceOf(TransactionTerminatedException.class);
            return null;
        });

        midPopulation.await();
        try (Transaction tx = db.beginTx()) {
            Result terminated = tx.execute("SHOW TRANSACTIONS YIELD metaData, transactionId AS txId WHERE "
                    + "metaData.terminateMe=true TERMINATE TRANSACTIONS txId YIELD message RETURN message");
            assertThat((String) terminated.columnAs("message").next()).contains("terminated");
        }
        midPopulation.release();

        constraintCreation.get();

        try (Transaction tx = db.beginTx()) {
            assertThat(Iterables.count(tx.schema().getConstraints())).isEqualTo(0);
            assertThat(Iterables.count(tx.schema().getIndexes())).isEqualTo(0);
        }
        assertThat(fs.fileExists(indexDir.get())).isFalse();
    }

    /**
     * This test describes a problem where if you crash during index population
     * when creating a constraint the constraint will never be created but the
     * index will and after next startup only the index will be there.
     *
     * We need to specifically drop the index in separate transaction to be
     * able to create the constraint again.
     *
     * A better behaviour would be that the index is never created and is not
     * present after crash.
     *
     * UPDATE: this problem sort of remains, except that we upon startup no longer
     * set the index as online; it will forever remain in a tentative "POPULATING"
     * state so that it won't be queried by an unsuspecting Cypher planner.
     */
    @Test
    void crashDuringIndexPopulationOfConstraint() throws InterruptedException {
        // Given
        trapPopulation.set(true);
        otherThread.execute(() -> {
            try (Transaction tx = db.beginTx()) {
                tx.schema()
                        .constraintFor(label)
                        .assertPropertyIsUnique(propertyKey)
                        .withName(nameA)
                        .create();
                tx.commit();
            }
            return null;
        });

        // When
        // Crashing during index population of index backing a constraint
        populationScanFinished.await();
        EphemeralFileSystemAbstraction crash = fs.snapshot();
        populationScanFinished.release();

        // Then
        // On next startup
        controller.restartDbms(builder -> {
            builder.setFileSystem(crash);
            return builder;
        });
        // the index exists but constraint is missing... which is sub-optimal
        final IndexingService indexingService =
                ((GraphDatabaseFacade) db).getDependencyResolver().resolveDependency(IndexingService.class);
        try (Transaction tx = db.beginTx()) {
            Iterable<ConstraintDefinition> constraints = tx.schema().getConstraints();
            Iterable<IndexDefinition> indexes = tx.schema().getIndexes();
            assertThat(count(constraints)).isEqualTo(0);
            assertThat(count(indexes)).isEqualTo(1);
            indexes.forEach(index -> {
                // Await index population job, but not necessarily that the index has come online.
                final IndexDescriptor descriptor = ((IndexDefinitionImpl) index).getIndexReference();
                try {
                    indexingService.getIndexProxy(descriptor).awaitStoreScanCompleted(1, HOURS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                final Schema.IndexState expectedState = index.getName().equals(nameA) ? POPULATING : ONLINE;
                assertThat(getIndexState(tx, index)).isEqualTo(expectedState);
            });
            tx.commit();
        }
        // and we cannot drop the constraint because it was never created
        {
            assertThatThrownBy(() -> {
                        try (Transaction tx = db.beginTx()) {
                            tx.execute("DROP CONSTRAINT `" + nameA + "`");
                            tx.commit();
                        }
                    })
                    .isInstanceOf(QueryExecutionException.class)
                    .hasRootCauseInstanceOf(NoSuchConstraintException.class);
        }
        // and we cannot re-create the constraint because index is blocking us
        {
            assertThatThrownBy(() -> {
                        try (Transaction tx = db.beginTx()) {
                            tx.schema()
                                    .constraintFor(label)
                                    .assertPropertyIsUnique(propertyKey)
                                    .withName(nameA)
                                    .create();
                            tx.commit();
                        }
                    })
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasRootCauseInstanceOf(IndexWithNameAlreadyExistsException.class);
        }

        // When
        // dropping the index in separate transaction
        try (Transaction tx = db.beginTx()) {
            tx.schema().getIndexByName(nameA).drop();
            tx.commit();
        }

        // Then
        // we can finally create the constraint again.
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(nameA)
                    .create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            Iterable<ConstraintDefinition> constraints = tx.schema().getConstraints();
            Iterable<IndexDefinition> indexes = tx.schema().getIndexes();
            assertThat(count(constraints)).isEqualTo(1);
            assertThat(count(indexes)).isEqualTo(1);
            indexes.forEach(index -> assertThat(getIndexState(tx, index)).isEqualTo(ONLINE));
            tx.commit();
        }
    }

    private static IndexDefinition getIndex(Transaction tx, String name) {
        return tx.schema().getIndexByName(name);
    }

    private static Schema.IndexState getIndexState(Transaction tx, IndexDefinition name) {
        return tx.schema().getIndexState(name);
    }

    private static Schema.IndexState getIndexState(Transaction tx, String name) {
        return getIndexState(tx, getIndex(tx, name));
    }

    private static String tooLargeString() {
        int violatingSize = DynamicSizeUtil.keyValueSizeCapFromPageSize(PageCache.PAGE_SIZE) + 1;
        return IndexEntryTestUtil.generateStringResultingInIndexEntrySize(violatingSize);
    }

    private static String alreadyExistsIndexMessage(String indexName) {
        return "There already exists an index called '" + indexName + "'.";
    }

    private static String thereAlreadyExistsConstraintMessage(String constraintName) {
        return "There already exists a constraint called '" + constraintName + "'.";
    }

    @Nested
    @ActorsExtension
    @ImpermanentDbmsExtension(configurationCallback = "configure")
    class SchemaConcurrency {
        @Inject
        Actor first;

        @Inject
        Actor second;

        BinaryLatch startLatch;

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            SchemaAcceptanceTest.this.configure(builder);
        }

        @BeforeEach
        void setUp() {
            startLatch = new BinaryLatch();
        }

        @RepeatedTest(20)
        void cannotCreateTokenIndexesWithTheSameSchemaInConcurrentTransactions() throws Exception {
            Future<Void> firstFuture = first.submit(schemaTransaction(
                    tx -> tx.schema().indexFor(AnyTokens.ANY_LABELS).withName("index-1")));
            Future<Void> secondFuture = second.submit(schemaTransaction(
                    tx -> tx.schema().indexFor(AnyTokens.ANY_LABELS).withName("index-2")));

            raceTransactions(firstFuture, secondFuture);

            assertOneSuccessAndOneFailure(firstFuture, secondFuture);
        }

        @RepeatedTest(20)
        void cannotCreateIndexesWithTheSameNameInConcurrentTransactions() throws Exception {
            String indexName = "MyIndex";

            Future<Void> firstFuture = first.submit(schemaTransaction(
                    tx -> tx.schema().indexFor(label).on(propertyKey).withName(indexName)));
            Future<Void> secondFuture = second.submit(schemaTransaction(
                    tx -> tx.schema().indexFor(otherLabel).on(secondPropertyKey).withName(indexName)));

            raceTransactions(firstFuture, secondFuture);

            assertOneSuccessAndOneFailure(firstFuture, secondFuture);
        }

        @RepeatedTest(20)
        void cannotCreateConstraintsWithTheSameNameInConcurrentTransactions() throws Exception {
            String constraintName = "MyConstraint";

            Future<Void> firstFuture = first.submit(schemaTransaction(tx -> tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(constraintName)));
            Future<Void> secondFuture = second.submit(schemaTransaction(tx -> tx.schema()
                    .constraintFor(otherLabel)
                    .assertPropertyIsUnique(secondPropertyKey)
                    .withName(constraintName)));

            raceTransactions(firstFuture, secondFuture);

            assertOneSuccessAndOneFailure(firstFuture, secondFuture);
        }

        @RepeatedTest(20)
        void cannotCreateIndexesAndConstraintsWithTheSameNameInConcurrentTransactions() throws Exception {
            String schemaName = "MySchema";

            Future<Void> firstFuture = first.submit(schemaTransaction(tx -> tx.schema()
                    .constraintFor(label)
                    .assertPropertyIsUnique(propertyKey)
                    .withName(schemaName)));
            Future<Void> secondFuture = second.submit(schemaTransaction(
                    tx -> tx.schema().indexFor(otherLabel).on(secondPropertyKey).withName(schemaName)));

            raceTransactions(firstFuture, secondFuture);

            assertOneSuccessAndOneFailure(firstFuture, secondFuture);
        }

        @Test
        void droppingConstraintMustLockNameForIndexCreate() throws Exception {
            String schemaName = "MySchema";
            createUniquenessConstraint(schemaName, label, propertyKey);
            try (Transaction tx = db.beginTx()) {
                tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
                tx.commit();
            }

            BinaryLatch afterFirstDropsConstraint = new BinaryLatch();
            BinaryLatch pauseFirst = new BinaryLatch();
            BinaryLatch beforeSecondCreatesIndex = new BinaryLatch();

            Future<Void> firstFuture = first.submit(() -> {
                try (Transaction tx = db.beginTx()) {
                    tx.schema().getConstraintByName(schemaName).drop();
                    afterFirstDropsConstraint.release();
                    pauseFirst.await();
                    tx.commit();
                }
            });
            Future<Void> secondFuture = second.submit(() -> {
                afterFirstDropsConstraint.await();
                try (Transaction tx = db.beginTx()) {
                    beforeSecondCreatesIndex.release();
                    IndexCreator indexCreator = tx.schema()
                            .indexFor(otherLabel)
                            .on(secondPropertyKey)
                            .withName(schemaName);
                    indexCreator.create();
                    tx.commit();
                }
            });

            first.untilWaitingIn(BinaryLatch.class.getMethod("await"));
            beforeSecondCreatesIndex.await();
            second.untilWaitingIn(ForsetiClient.class.getMethod("incrementalBackoffWait", long.class));
            second.untilWaiting();
            pauseFirst.release();
            firstFuture.get();
            secondFuture.get();
            try (Transaction tx = db.beginTx()) {
                assertThat(tx.schema().getConstraints()).isEmpty();
                final var indexes = tx.schema().getIndexes().iterator();
                assertThat(indexes).hasNext();
                assertThat(indexes.next().getName()).isEqualTo(schemaName);
                assertThat(indexes).isExhausted();
                tx.commit();
            }
        }

        @RepeatedTest(10)
        void awaitIndexesMustNotThrowOnConcurrentlyDroppedIndexes() throws Exception {
            AtomicBoolean stop = new AtomicBoolean();
            Queue<IndexDefinition> indexes = new ConcurrentLinkedQueue<>();

            try (Transaction tx = db.beginTx()) {
                for (int i = 0; i < 50; i++) {
                    indexes.add(tx.schema()
                            .indexFor(Label.label("Label_" + i))
                            .on("propl_" + i)
                            .create());
                }
                tx.commit();
            }

            Future<Void> firstFuture = first.submit(() -> {
                startLatch.await();
                while (!stop.get()) {
                    try (Transaction tx = db.beginTx()) {
                        try {
                            tx.schema().awaitIndexesOnline(20, TimeUnit.MINUTES);
                        } catch (DeadlockDetectedException de) {
                            // retry
                        } catch (Exception e) {
                            stop.set(true);
                            indexes.clear();
                            throw e;
                        }
                        tx.commit();
                    }
                }
            });

            Future<Void> secondFuture = second.submit(() -> {
                startLatch.await();
                IndexDefinition index;
                try {
                    while ((index = indexes.poll()) != null) {
                        try (Transaction tx = db.beginTx()) {
                            Thread.sleep(1);
                            getIndex(tx, index.getName()).drop();
                            tx.commit();
                        } catch (DeadlockDetectedException ignore) {
                            // ignore
                        }
                    }
                } finally {
                    stop.set(true);
                }
                return null;
            });

            raceTransactions(firstFuture, secondFuture);

            // Then these must not throw.
            firstFuture.get();
            secondFuture.get();
        }

        private Callable<Void> schemaTransaction(ThrowingFunction<Transaction, Object, Exception> action) {
            return () -> {
                try (Transaction tx = db.beginTx()) {
                    Object creator = action.apply(tx);
                    startLatch.await();
                    if (creator instanceof IndexCreator) {
                        ((IndexCreator) creator).create();
                    } else if (creator instanceof ConstraintCreator) {
                        ((ConstraintCreator) creator).create();
                    } else {
                        fail("Don't know how to create from " + creator);
                    }
                    tx.commit();
                }
                return null;
            };
        }

        private void raceTransactions(Future<Void> firstFuture, Future<Void> secondFuture)
                throws InterruptedException, NoSuchMethodException {
            first.untilWaitingIn(BinaryLatch.class.getMethod("await"));
            second.untilWaitingIn(BinaryLatch.class.getMethod("await"));
            startLatch.release();

            while (!firstFuture.isDone() || !secondFuture.isDone()) {
                Thread.onSpinWait();
            }
        }

        private void assertOneSuccessAndOneFailure(Future<Void> firstFuture, Future<Void> secondFuture)
                throws InterruptedException {
            Throwable firstThrowable = getException(firstFuture);
            Throwable secondThrowable = getException(secondFuture);
            if (firstThrowable == null && secondThrowable == null) {
                fail("Both transactions completed successfully, when one of them should have thrown.");
            }
            Throwable error = firstThrowable != null ? firstThrowable : secondThrowable;
            // The most common exception is to notice the duplicate rule/name at transaction creation time, however
            // there's a miniscule chance that
            // both transactions will progress a bit longer side by side and one of them instead tripping on a check
            // that says that transactions
            // cannot commit if there has been a constraint created while the transaction was running.
            assertThat(error).isInstanceOfAny(ConstraintViolationException.class, TransactionFailureException.class);
        }

        private Throwable getException(Future<Void> future) throws InterruptedException {
            try {
                future.get();
                return null;
            } catch (ExecutionException e) {
                return e.getCause();
            }
        }
    }

    private static void dropConstraint(GraphDatabaseService db, ConstraintDefinition constraint) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraintByName(constraint.getName()).drop();
            tx.commit();
        }
    }

    private ConstraintDefinition createUniquenessConstraint(Label label, String... properties) {
        return createUniquenessConstraint(null, label, properties);
    }

    private ConstraintDefinition createUniquenessConstraint(String name, Label label, String... properties) {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator = tx.schema().constraintFor(label);

            for (String property : properties) {
                constraintCreator = constraintCreator.assertPropertyIsUnique(property);
            }

            constraintCreator = constraintCreator.withName(name);
            ConstraintDefinition constraint = constraintCreator.create();
            tx.commit();
            return constraint;
        }
    }

    private ConstraintDefinition createRelUniquenessConstraint(RelationshipType type, String... properties) {
        return createRelUniquenessConstraint(null, type, properties);
    }

    private ConstraintDefinition createRelUniquenessConstraint(
            String name, RelationshipType type, String... properties) {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator constraintCreator = tx.schema().constraintFor(type);

            for (String property : properties) {
                constraintCreator = constraintCreator.assertPropertyIsUnique(property);
            }

            constraintCreator = constraintCreator.withName(name);
            ConstraintDefinition constraint = constraintCreator.create();
            tx.commit();
            return constraint;
        }
    }

    protected void dropIndex(IndexDefinition index) {
        try (Transaction tx = db.beginTx()) {
            getIndex(tx, index.getName()).drop();
            tx.commit();
        }
    }

    static Node createNode(GraphDatabaseService db, String key, Object value, Label label) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(label);
            node.setProperty(key, value);
            tx.commit();
            return node;
        }
    }

    public static IndexDefinition createIndex(GraphDatabaseService db, RelationshipType relType, String... properties) {
        return createIndex(db, RANGE, null, relType, properties);
    }

    public static IndexDefinition createIndex(
            GraphDatabaseService db, IndexType indexType, RelationshipType relType, String... properties) {
        return createIndex(db, indexType, null, relType, properties);
    }

    public static IndexDefinition createIndex(
            GraphDatabaseService db, IndexType indexType, String name, RelationshipType relType, String... properties) {
        IndexDefinition indexDef = createIndexNoWait(db, indexType, name, relType, properties);
        waitForIndex(db, indexDef);
        return indexDef;
    }

    static IndexDefinition createIndexNoWait(
            GraphDatabaseService db, IndexType indexType, String name, RelationshipType relType, String... properties) {
        IndexDefinition indexDef;
        try (Transaction tx = db.beginTx()) {
            IndexCreator indexCreator = tx.schema().indexFor(relType).withIndexType(indexType);
            for (String property : properties) {
                indexCreator = indexCreator.on(property);
            }
            if (name != null) {
                indexCreator = indexCreator.withName(name);
            }
            indexDef = indexCreator.create();
            tx.commit();
        }
        return indexDef;
    }

    public static IndexDefinition createIndex(
            GraphDatabaseService db, IndexType indexType, Label label, String... properties) {
        return createIndex(db, indexType, null, label, properties);
    }

    public static IndexDefinition createIndex(GraphDatabaseService db, Label label, String... properties) {
        return createIndex(db, RANGE, null, label, properties);
    }

    public static IndexDefinition createIndex(GraphDatabaseService db, String name, Label label, String... properties) {
        return createIndex(db, RANGE, name, label, properties);
    }

    public static IndexDefinition createIndex(
            GraphDatabaseService db, IndexType indexType, String name, Label label, String... properties) {
        IndexDefinition indexDef = createIndexNoWait(db, indexType, name, label, properties);
        waitForIndex(db, indexDef);
        return indexDef;
    }

    public static IndexDefinition createIndex(GraphDatabaseService db, AnyTokens tokens, String name) {
        IndexDefinition index;
        try (var tx = db.beginTx()) {
            IndexCreator indexCreator = tx.schema().indexFor(tokens);
            if (name != null) {
                indexCreator = indexCreator.withName(name);
            }
            index = indexCreator.create();
            tx.commit();
        }
        waitForIndex(db, index);
        return index;
    }

    static IndexDefinition createIndexNoWait(
            GraphDatabaseService db, IndexType indexType, String name, Label label, String... properties) {
        IndexDefinition indexDef;
        try (Transaction tx = db.beginTx()) {
            IndexCreator indexCreator = tx.schema().indexFor(label).withIndexType(indexType);
            for (String property : properties) {
                indexCreator = indexCreator.on(property);
            }
            if (name != null) {
                indexCreator = indexCreator.withName(name);
            }
            indexDef = indexCreator.create();
            tx.commit();
        }
        return indexDef;
    }

    static void waitForIndex(GraphDatabaseService beansAPI, IndexDefinition indexDef) {
        try (Transaction tx = beansAPI.beginTx()) {
            tx.schema().awaitIndexOnline(indexDef, 10, MINUTES);
        }
    }

    private static Iterable<IndexDefinition> getIndexes(Transaction tx, RelationshipType relType) {
        return tx.schema().getIndexes(relType);
    }

    public static void waitForIndexes(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(10, MINUTES);
        }
    }

    private static Iterable<IndexDefinition> getIndexes(Transaction tx, Label label) {
        return tx.schema().getIndexes(label);
    }
}
