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
package org.neo4j.graphdb.schema;

import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.assertj.core.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextProcedureKernelTransaction;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextProcedureTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

/**
 * Use @TestInstance( TestInstance.Lifecycle.PER_CLASS ) to not setup
 * new dbms for every test. Instead reuse same db and clean it between
 * every round. This shortens test execution a lot!
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DbmsExtension(configurationCallback = "configuration")
@ExtendWith(RandomExtension.class)
public class FindEntityByTokenAndPropertyIT {
    private static final String TOKEN = "token";
    private static final String PROPERTY_KEY = "prop";
    private static final String PROPERTY_KEY_2 = "prop2";
    private static final String PROPERTY_KEY_3 = "prop3";
    private static final String[] PROPERTY_KEYS = new String[] {PROPERTY_KEY, PROPERTY_KEY_2, PROPERTY_KEY_3};
    private final MyIndexMonitor indexMonitor = new MyIndexMonitor();

    @Inject
    private GraphDatabaseService db;

    @Inject
    private RandomSupport random;

    @ExtensionCallback
    void configuration(TestDatabaseManagementServiceBuilder builder) {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener(indexMonitor);
        builder.setMonitors(monitors);
    }

    @BeforeEach
    void cleanDb() {
        // Clean reused db between every test
        try (Transaction tx = db.beginTx()) {
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
        try (Transaction tx = db.beginTx();
                ResourceIterable<Relationship> allRelationships = tx.getAllRelationships()) {
            allRelationships.forEach(Relationship::delete);
            tx.commit();
        }
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            allNodes.forEach(Node::delete);
            tx.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("indexCompatibilities")
    void shouldUseIndexWhenFindingEntityWithIndexCompatiblePropertyValue(
            EntityCreator entityCreator, FindMethod findMethod, SupportedIndexType firstIndex) {
        // Given
        Object value = random.nextValue().asObject();
        Entity entity = createEntity(entityCreator, value);
        IndexDescriptor firstDescriptor;
        try (Transaction tx = db.beginTx()) {
            firstDescriptor = entityCreator.createIndex(tx, firstIndex.indexType(), TOKEN, PROPERTY_KEY);
            tx.commit();
        }
        awaitIndexes();
        indexMonitor.clear();

        try (Transaction tx = db.beginTx()) {
            // When
            ResourceIterator<? extends Entity> result = findMethod.find(tx, TOKEN, PROPERTY_KEY, value);

            // Then
            assertFoundEntity(entity, result);
        }

        // Then
        IndexDescriptor[] expectedDescriptors = expectedDescriptors(firstDescriptor, firstIndex.supportedType(value));
        validateUsedExpectedIndex("exact match single property: " + valueAsString(value), expectedDescriptors);
    }

    @ParameterizedTest
    @MethodSource("indexCompatibilitiesMultiIndex")
    void shouldUseIndexWhenFindingEntityWithIndexCompatiblePropertyValueMultipleIndexes(
            EntityCreator entityCreator,
            FindMethod findMethod,
            SupportedIndexType firstIndex,
            SupportedIndexType secondIndex) {
        // Given
        Object value = random.nextValue().asObject();
        Entity entity = createEntity(entityCreator, value);
        IndexDescriptor firstDescriptor;
        IndexDescriptor secondDescriptor;
        try (Transaction tx = db.beginTx()) {
            firstDescriptor = entityCreator.createIndex(tx, firstIndex.indexType(), TOKEN, PROPERTY_KEY);
            secondDescriptor = entityCreator.createIndex(tx, secondIndex.indexType(), TOKEN, PROPERTY_KEY);
            tx.commit();
        }
        awaitIndexes();
        indexMonitor.clear();

        try (Transaction tx = db.beginTx()) {
            // When
            ResourceIterator<? extends Entity> result = findMethod.find(tx, TOKEN, PROPERTY_KEY, value);

            // Then
            assertFoundEntity(entity, result);
        }

        // Then
        IndexDescriptor[] expectedDescriptors = expectedDescriptors(
                firstDescriptor, secondDescriptor, firstIndex.supportedType(value), secondIndex.supportedType(value));
        validateUsedExpectedIndex("exact match single property: " + valueAsString(value), expectedDescriptors);
    }

    @ParameterizedTest
    @MethodSource("indexCompatibilitiesComposite2")
    void shouldUseIndexWhenFindingEntityWithIndexCompatiblePropertyValueCompositeQuery2(
            EntityCreator entityCreator, FindMethod findMethod, SupportedIndexType firstIndex) {
        // Given
        Object value1 = random.nextValue().asObject();
        Object value2 = random.nextValue().asObject();
        Entity entity = createEntity(entityCreator, value1, value2);
        IndexDescriptor firstDescriptor;
        try (Transaction tx = db.beginTx()) {
            firstDescriptor =
                    entityCreator.createIndex(tx, firstIndex.indexType(), TOKEN, PROPERTY_KEY, PROPERTY_KEY_2);
            tx.commit();
        }
        awaitIndexes();
        indexMonitor.clear();

        try (Transaction tx = db.beginTx()) {
            // When
            ResourceIterator<? extends Entity> result =
                    findMethod.find(tx, TOKEN, PROPERTY_KEY, value1, PROPERTY_KEY_2, value2);

            // Then
            assertFoundEntity(entity, result);
        }

        // Then
        IndexDescriptor[] expectedDescriptors =
                expectedDescriptors(firstDescriptor, firstIndex.supportedType(value1, value2));
        validateUsedExpectedIndex(
                "exact match composite property: " + compositeValueString(value1, value2), expectedDescriptors);
    }

    @ParameterizedTest
    @MethodSource("indexCompatibilitiesComposite2MultiIndex")
    void shouldUseIndexWhenFindingEntityWithIndexCompatiblePropertyValueCompositeQuery2MultiIndex(
            EntityCreator entityCreator,
            FindMethod findMethod,
            SupportedIndexType firstIndex,
            SupportedIndexType secondIndex) {
        // Given
        Object value1 = random.nextValue().asObject();
        Object value2 = random.nextValue().asObject();
        Entity entity = createEntity(entityCreator, value1, value2);
        IndexDescriptor firstDescriptor;
        IndexDescriptor secondDescriptor;
        try (Transaction tx = db.beginTx()) {
            firstDescriptor =
                    entityCreator.createIndex(tx, firstIndex.indexType(), TOKEN, PROPERTY_KEY, PROPERTY_KEY_2);
            secondDescriptor =
                    entityCreator.createIndex(tx, secondIndex.indexType(), TOKEN, PROPERTY_KEY, PROPERTY_KEY_2);
            tx.commit();
        }
        awaitIndexes();
        indexMonitor.clear();

        try (Transaction tx = db.beginTx()) {
            // When
            ResourceIterator<? extends Entity> result =
                    findMethod.find(tx, TOKEN, PROPERTY_KEY, value1, PROPERTY_KEY_2, value2);

            // Then
            assertFoundEntity(entity, result);
        }

        // Then
        IndexDescriptor[] expectedDescriptors = expectedDescriptors(
                firstDescriptor,
                secondDescriptor,
                firstIndex.supportedType(value1, value2),
                secondIndex.supportedType(value1, value2));
        validateUsedExpectedIndex(
                "exact match composite property: " + compositeValueString(value1, value2), expectedDescriptors);
    }

    @ParameterizedTest
    @MethodSource("indexCompatibilitiesComposite3")
    void shouldUseIndexWhenFindingEntityWithIndexCompatiblePropertyValueCompositeQuery3(
            EntityCreator entityCreator, FindMethod findMethod, SupportedIndexType firstIndex) {
        // Given
        Object value1 = random.nextValue().asObject();
        Object value2 = random.nextValue().asObject();
        Object value3 = random.nextValue().asObject();
        Entity entity = createEntity(entityCreator, value1, value2, value3);
        IndexDescriptor firstDescriptor;
        try (Transaction tx = db.beginTx()) {
            firstDescriptor = entityCreator.createIndex(
                    tx, firstIndex.indexType(), TOKEN, PROPERTY_KEY, PROPERTY_KEY_2, PROPERTY_KEY_3);
            tx.commit();
        }
        awaitIndexes();
        indexMonitor.clear();

        try (Transaction tx = db.beginTx()) {
            // When
            ResourceIterator<? extends Entity> result =
                    findMethod.find(tx, TOKEN, PROPERTY_KEY, value1, PROPERTY_KEY_2, value2, PROPERTY_KEY_3, value3);

            // Then
            assertFoundEntity(entity, result);
        }

        // Then
        IndexDescriptor[] expectedDescriptors =
                expectedDescriptors(firstDescriptor, firstIndex.supportedType(value1, value2, value3));
        validateUsedExpectedIndex(
                "exact match composite property: " + compositeValueString(value1, value2, value3), expectedDescriptors);
    }

    @ParameterizedTest
    @MethodSource("indexCompatibilitiesComposite3MultiIndex")
    void shouldUseIndexWhenFindingEntityWithIndexCompatiblePropertyValueCompositeQuery3MultiIndex(
            EntityCreator entityCreator,
            FindMethod findMethod,
            SupportedIndexType firstIndex,
            SupportedIndexType secondIndex) {
        // Given
        Object value1 = random.nextValue().asObject();
        Object value2 = random.nextValue().asObject();
        Object value3 = random.nextValue().asObject();
        Entity entity = createEntity(entityCreator, value1, value2, value3);
        IndexDescriptor firstDescriptor;
        IndexDescriptor secondDescriptor;
        try (Transaction tx = db.beginTx()) {
            firstDescriptor = entityCreator.createIndex(
                    tx, firstIndex.indexType(), TOKEN, PROPERTY_KEY, PROPERTY_KEY_2, PROPERTY_KEY_3);
            secondDescriptor = entityCreator.createIndex(
                    tx, secondIndex.indexType(), TOKEN, PROPERTY_KEY, PROPERTY_KEY_2, PROPERTY_KEY_3);
            tx.commit();
        }
        awaitIndexes();
        indexMonitor.clear();

        try (Transaction tx = db.beginTx()) {
            // When
            ResourceIterator<? extends Entity> result =
                    findMethod.find(tx, TOKEN, PROPERTY_KEY, value1, PROPERTY_KEY_2, value2, PROPERTY_KEY_3, value3);

            // Then
            assertFoundEntity(entity, result);
        }

        // Then
        IndexDescriptor[] expectedDescriptors = expectedDescriptors(
                firstDescriptor,
                secondDescriptor,
                firstIndex.supportedType(value1, value2, value3),
                secondIndex.supportedType(value1, value2, value3));
        validateUsedExpectedIndex(
                "exact match composite property: " + compositeValueString(value1, value2, value3), expectedDescriptors);
    }

    @ParameterizedTest
    @MethodSource("indexCompatibilitiesStringSearch")
    void shouldUseIndexWhenFindingEntityWithIndexCompatibleStringSearch(
            EntityCreator entityCreator,
            FindMethod findMethod,
            SupportedIndexType firstIndexType,
            SearchMode searchMode) {
        // String need to be at least 4 char long to create all possible templates
        // Use BMP strings, otherwise symbols will be cut in the middle when String.substring(...)
        String value =
                random.randomValues().nextBasicMultilingualPlaneTextValue(4, 20).stringValue();
        String template = searchMode.asTemplate(value);
        StringSearchMode stringSearchMode = searchMode.mode();
        Entity entity = createEntity(entityCreator, value);
        IndexDescriptor firstDescriptor;
        try (Transaction tx = db.beginTx()) {
            firstDescriptor = entityCreator.createIndex(tx, firstIndexType.indexType(), TOKEN, PROPERTY_KEY);
            tx.commit();
        }
        awaitIndexes();
        indexMonitor.clear();

        try (Transaction tx = db.beginTx()) {
            // When
            ResourceIterator<? extends Entity> result =
                    findMethod.find(tx, TOKEN, PROPERTY_KEY, template, stringSearchMode);

            // Then
            assertFoundEntity(entity, result);
        }

        // Then
        IndexDescriptor[] expectedDescriptors =
                expectedDescriptors(firstDescriptor, firstIndexType.supportedStringSearch(stringSearchMode));
        validateUsedExpectedIndex(
                "string search: " + stringSearchMode + " on template " + template + ", expecting "
                        + valueAsString(value),
                expectedDescriptors);
    }

    @ParameterizedTest
    @MethodSource("indexCompatibilitiesStringSearchMultiIndex")
    void shouldUseIndexWhenFindingEntityWithIndexCompatibleStringSearchMultiIndex(
            EntityCreator entityCreator,
            FindMethod findMethod,
            SupportedIndexType firstIndex,
            SupportedIndexType secondIndex,
            SearchMode searchMode) {
        // Given
        // String need to be at least 4 char long to create all possible templates
        // Use BMP strings, otherwise symbols will be cut in the middle when String.substring(...)
        String value =
                random.randomValues().nextBasicMultilingualPlaneTextValue(4, 20).stringValue();
        String template = searchMode.asTemplate(value);
        StringSearchMode stringSearchMode = searchMode.mode();
        Entity entity = createEntity(entityCreator, value);
        IndexDescriptor firstDescriptor;
        IndexDescriptor secondDescriptor;
        try (Transaction tx = db.beginTx()) {
            firstDescriptor = entityCreator.createIndex(tx, firstIndex.indexType(), TOKEN, PROPERTY_KEY);
            secondDescriptor = entityCreator.createIndex(tx, secondIndex.indexType(), TOKEN, PROPERTY_KEY);
            tx.commit();
        }
        awaitIndexes();
        indexMonitor.clear();

        try (Transaction tx = db.beginTx()) {
            // When
            ResourceIterator<? extends Entity> result =
                    findMethod.find(tx, TOKEN, PROPERTY_KEY, template, stringSearchMode);

            // Then
            assertFoundEntity(entity, result);
        }

        // Then
        IndexDescriptor[] expectedDescriptors = expectedDescriptors(
                firstDescriptor,
                secondDescriptor,
                firstIndex.supportedStringSearch(stringSearchMode),
                secondIndex.supportedStringSearch(stringSearchMode));
        validateUsedExpectedIndex(
                "string search: " + stringSearchMode + " on template " + template + ", expecting "
                        + valueAsString(value),
                expectedDescriptors);
    }

    private Entity createEntity(EntityCreator entityCreator, Object... values) {
        Entity entity;
        try (Transaction tx = db.beginTx()) {
            entity = entityCreator.createEntity(tx, TOKEN);
            for (int i = 0; i < values.length; i++) {
                entity.setProperty(PROPERTY_KEYS[i], values[i]);
            }
            tx.commit();
        }
        return entity;
    }

    private void validateUsedExpectedIndex(String queryDescription, IndexDescriptor... anyExpectedIndexDescriptor) {
        boolean expectedToUseIndex = anyExpectedIndexDescriptor.length > 0;
        assertThat(indexMonitor.queriedIndex)
                .as("used an index for " + queryDescription)
                .isEqualTo(expectedToUseIndex);
        if (expectedToUseIndex) {
            assertThat(anyExpectedIndexDescriptor)
                    .as("used any of expected index for " + queryDescription)
                    .contains(indexMonitor.descriptor);
        }
    }

    private String valueAsString(Object value) {
        if (Arrays.isArray(value)) {
            return ArrayUtils.toString(value);
        }
        return value.toString();
    }

    private String compositeValueString(Object... values) {
        StringJoiner joiner = new StringJoiner(" AND ");
        stream(values).forEach(v -> joiner.add(valueAsString(v)));
        return joiner.toString();
    }

    private void awaitIndexes() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            tx.commit();
        }
    }

    private static void assertFoundEntity(Entity entity, ResourceIterator<? extends Entity> result) {
        assertThat(result).as("result iterator").isNotNull();
        assertThat(result).as("result iterator").hasNext();
        assertThat(result.next()).as("entity").isEqualTo(entity);
        assertThat(result).as("result iterator").isExhausted();
    }

    private static IndexDescriptor[] expectedDescriptors(
            IndexDescriptor firstDescriptor, boolean firstIndexSupportType) {
        return firstIndexSupportType ? new IndexDescriptor[] {firstDescriptor} : new IndexDescriptor[0];
    }

    private static IndexDescriptor[] expectedDescriptors(
            IndexDescriptor firstDescriptor,
            IndexDescriptor secondDescriptor,
            boolean firstIndexSupportType,
            boolean secondIndexSupportType) {
        List<IndexDescriptor> expectedIndexes = new ArrayList<>();
        if (firstIndexSupportType) {
            expectedIndexes.add(firstDescriptor);
        }
        if (secondIndexSupportType) {
            expectedIndexes.add(secondDescriptor);
        }
        return expectedIndexes.toArray(IndexDescriptor[]::new);
    }

    public static Stream<Arguments> indexCompatibilities() {
        List<Arguments> arguments = new ArrayList<>();
        for (SupportedIndexType index : SupportedIndexType.values()) {
            arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.singleNode, index));
            arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.singleNodeFromExecutionContext, index));
            arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.multipleNodes, index));
            arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesFromExecutionContext, index));
            arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesMap, index));
            arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesFromExecutionContextMap, index));
            arguments.add(Arguments.of(EntityCreator.RELATIONSHIP, FindMethod.singleRelationship, index));
            arguments.add(
                    Arguments.of(EntityCreator.RELATIONSHIP, FindMethod.singleRelationshipFromExecutionContext, index));
            arguments.add(Arguments.of(EntityCreator.RELATIONSHIP, FindMethod.multipleRelationships, index));
            arguments.add(Arguments.of(
                    EntityCreator.RELATIONSHIP, FindMethod.multipleRelationshipsFromExecutionContext, index));
            arguments.add(Arguments.of(EntityCreator.RELATIONSHIP, FindMethod.multipleRelationshipsMap, index));
            arguments.add(Arguments.of(
                    EntityCreator.RELATIONSHIP, FindMethod.multipleRelationshipsFromExecutionContextMap, index));
        }
        return arguments.stream();
    }

    public static Stream<Arguments> indexCompatibilitiesMultiIndex() {
        List<Arguments> arguments = new ArrayList<>();
        for (SupportedIndexType firstIndex : SupportedIndexType.values()) {
            for (SupportedIndexType secondIndex : SupportedIndexType.values()) {
                if (firstIndex != secondIndex) {
                    arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.singleNode, firstIndex, secondIndex));
                    arguments.add(Arguments.of(
                            EntityCreator.NODE, FindMethod.singleNodeFromExecutionContext, firstIndex, secondIndex));
                    arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.multipleNodes, firstIndex, secondIndex));
                    arguments.add(Arguments.of(
                            EntityCreator.NODE, FindMethod.multipleNodesFromExecutionContext, firstIndex, secondIndex));
                    arguments.add(
                            Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesMap, firstIndex, secondIndex));
                    arguments.add(Arguments.of(
                            EntityCreator.NODE,
                            FindMethod.multipleNodesFromExecutionContextMap,
                            firstIndex,
                            secondIndex));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP, FindMethod.singleRelationship, firstIndex, secondIndex));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP,
                            FindMethod.singleRelationshipFromExecutionContext,
                            firstIndex,
                            secondIndex));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP, FindMethod.multipleRelationships, firstIndex, secondIndex));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP,
                            FindMethod.multipleRelationshipsFromExecutionContext,
                            firstIndex,
                            secondIndex));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP, FindMethod.multipleRelationshipsMap, firstIndex, secondIndex));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP,
                            FindMethod.multipleRelationshipsFromExecutionContextMap,
                            firstIndex,
                            secondIndex));
                }
            }
        }
        return arguments.stream();
    }

    public static Stream<Arguments> indexCompatibilitiesComposite2() {
        List<Arguments> arguments = new ArrayList<>();
        stream(SupportedIndexType.values())
                .filter(SupportedIndexType::supportCompositeIndex)
                .forEach(index -> {
                    arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesComposite2, index));
                    arguments.add(Arguments.of(
                            EntityCreator.NODE, FindMethod.multipleNodesFromExecutionContextComposite2, index));
                    arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesMap, index));
                    arguments.add(
                            Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesFromExecutionContextMap, index));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP, FindMethod.multipleRelationshipsComposite2, index));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP,
                            FindMethod.multipleRelationshipsFromExecutionContextComposite2,
                            index));
                    arguments.add(Arguments.of(EntityCreator.RELATIONSHIP, FindMethod.multipleRelationshipsMap, index));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP,
                            FindMethod.multipleRelationshipsFromExecutionContextMap,
                            index));
                });
        return arguments.stream();
    }

    public static Stream<Arguments> indexCompatibilitiesComposite2MultiIndex() {
        List<Arguments> arguments = new ArrayList<>();
        stream(SupportedIndexType.values())
                .filter(SupportedIndexType::supportCompositeIndex)
                .forEach(firstIndex -> stream(SupportedIndexType.values())
                        .filter(SupportedIndexType::supportCompositeIndex)
                        .forEach(secondIndex -> {
                            if (firstIndex != secondIndex) {
                                arguments.add(Arguments.of(
                                        EntityCreator.NODE,
                                        FindMethod.multipleNodesComposite2,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.NODE,
                                        FindMethod.multipleNodesFromExecutionContextComposite2,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.NODE, FindMethod.multipleNodesMap, firstIndex, secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.NODE,
                                        FindMethod.multipleNodesFromExecutionContextMap,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.RELATIONSHIP,
                                        FindMethod.multipleRelationshipsComposite2,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.RELATIONSHIP,
                                        FindMethod.multipleRelationshipsFromExecutionContextComposite2,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.RELATIONSHIP,
                                        FindMethod.multipleRelationshipsMap,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.RELATIONSHIP,
                                        FindMethod.multipleRelationshipsFromExecutionContextMap,
                                        firstIndex,
                                        secondIndex));
                            }
                        }));
        return arguments.stream();
    }

    public static Stream<Arguments> indexCompatibilitiesComposite3() {
        List<Arguments> arguments = new ArrayList<>();
        stream(SupportedIndexType.values())
                .filter(SupportedIndexType::supportCompositeIndex)
                .forEach(index -> {
                    arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesComposite3, index));
                    arguments.add(Arguments.of(
                            EntityCreator.NODE, FindMethod.multipleNodesFromExecutionContextComposite3, index));
                    arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesMap, index));
                    arguments.add(
                            Arguments.of(EntityCreator.NODE, FindMethod.multipleNodesFromExecutionContextMap, index));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP, FindMethod.multipleRelationshipsComposite3, index));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP,
                            FindMethod.multipleRelationshipsFromExecutionContextComposite3,
                            index));
                    arguments.add(Arguments.of(EntityCreator.RELATIONSHIP, FindMethod.multipleRelationshipsMap, index));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP,
                            FindMethod.multipleRelationshipsFromExecutionContextMap,
                            index));
                });
        return arguments.stream();
    }

    public static Stream<Arguments> indexCompatibilitiesComposite3MultiIndex() {
        List<Arguments> arguments = new ArrayList<>();
        stream(SupportedIndexType.values())
                .filter(SupportedIndexType::supportCompositeIndex)
                .forEach(firstIndex -> stream(SupportedIndexType.values())
                        .filter(SupportedIndexType::supportCompositeIndex)
                        .forEach(secondIndex -> {
                            if (firstIndex != secondIndex) {
                                arguments.add(Arguments.of(
                                        EntityCreator.NODE,
                                        FindMethod.multipleNodesComposite3,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.NODE,
                                        FindMethod.multipleNodesFromExecutionContextComposite3,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.NODE, FindMethod.multipleNodesMap, firstIndex, secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.NODE,
                                        FindMethod.multipleNodesFromExecutionContextMap,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.RELATIONSHIP,
                                        FindMethod.multipleRelationshipsComposite3,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.RELATIONSHIP,
                                        FindMethod.multipleRelationshipsFromExecutionContextComposite3,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.RELATIONSHIP,
                                        FindMethod.multipleRelationshipsMap,
                                        firstIndex,
                                        secondIndex));
                                arguments.add(Arguments.of(
                                        EntityCreator.RELATIONSHIP,
                                        FindMethod.multipleRelationshipsFromExecutionContextMap,
                                        firstIndex,
                                        secondIndex));
                            }
                        }));
        return arguments.stream();
    }

    public static Stream<Arguments> indexCompatibilitiesStringSearch() {
        List<Arguments> arguments = new ArrayList<>();
        stream(SupportedIndexType.values())
                .forEach(index -> stream(SearchMode.values()).forEach(searchMode -> {
                    arguments.add(Arguments.of(EntityCreator.NODE, FindMethod.stringSearchNodes, index, searchMode));
                    arguments.add(Arguments.of(
                            EntityCreator.NODE, FindMethod.stringSearchNodesFromExecutionContext, index, searchMode));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP, FindMethod.stringSearchRelationships, index, searchMode));
                    arguments.add(Arguments.of(
                            EntityCreator.RELATIONSHIP,
                            FindMethod.stringSearchRelationshipsFromExecutionContext,
                            index,
                            searchMode));
                }));
        return arguments.stream();
    }

    public static Stream<Arguments> indexCompatibilitiesStringSearchMultiIndex() {
        List<Arguments> arguments = new ArrayList<>();
        stream(SupportedIndexType.values())
                .forEach(firstIndex -> stream(SupportedIndexType.values()).forEach(secondIndex -> {
                    if (firstIndex != secondIndex) {
                        stream(SearchMode.values()).forEach(searchMode -> {
                            arguments.add(Arguments.of(
                                    EntityCreator.NODE,
                                    FindMethod.stringSearchNodes,
                                    firstIndex,
                                    secondIndex,
                                    searchMode));
                            arguments.add(Arguments.of(
                                    EntityCreator.NODE,
                                    FindMethod.stringSearchNodesFromExecutionContext,
                                    firstIndex,
                                    secondIndex,
                                    searchMode));
                            arguments.add(Arguments.of(
                                    EntityCreator.RELATIONSHIP,
                                    FindMethod.stringSearchRelationships,
                                    firstIndex,
                                    secondIndex,
                                    searchMode));
                            arguments.add(Arguments.of(
                                    EntityCreator.RELATIONSHIP,
                                    FindMethod.stringSearchRelationshipsFromExecutionContext,
                                    firstIndex,
                                    secondIndex,
                                    searchMode));
                        });
                    }
                }));
        return arguments.stream();
    }

    private static Transaction executionContextTransaction(Transaction tx) {
        var internalTx = ((InternalTransaction) tx);
        var ktx = internalTx.kernelTransaction();
        internalTx.registerCloseableResource(ktx.acquireStatement());
        var executionContext = ktx.createExecutionContext();
        internalTx.registerCloseableResource(() -> {
            executionContext.complete();
            executionContext.close();
        });
        return new ExecutionContextProcedureTransaction(
                new ExecutionContextProcedureKernelTransaction(ktx, executionContext), null);
    }

    private enum FindMethod {
        singleNode {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return Iterators.asResourceIterator(
                        Collections.<Entity>singletonList(tx.findNode(Label.label(token), propertyKey, propertyValue)));
            }
        },
        singleNodeFromExecutionContext {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return Iterators.asResourceIterator(Collections.<Entity>singletonList(
                        executionContextTransaction(tx).findNode(Label.label(token), propertyKey, propertyValue)));
            }
        },
        multipleNodes {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return tx.findNodes(Label.label(token), propertyKey, propertyValue);
            }
        },
        multipleNodesFromExecutionContext {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return executionContextTransaction(tx).findNodes(Label.label(token), propertyKey, propertyValue);
            }
        },

        multipleNodesComposite2 {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2) {
                return tx.findNodes(Label.label(token), propertyKey1, propertyValue1, propertyKey2, propertyValue2);
            }
        },

        multipleNodesFromExecutionContextComposite2 {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2) {
                return executionContextTransaction(tx)
                        .findNodes(Label.label(token), propertyKey1, propertyValue1, propertyKey2, propertyValue2);
            }
        },

        multipleNodesComposite3 {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2,
                    String propertyKey3,
                    Object propertyValue3) {
                return tx.findNodes(
                        Label.label(token),
                        propertyKey1,
                        propertyValue1,
                        propertyKey2,
                        propertyValue2,
                        propertyKey3,
                        propertyValue3);
            }
        },

        multipleNodesFromExecutionContextComposite3 {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2,
                    String propertyKey3,
                    Object propertyValue3) {
                return executionContextTransaction(tx)
                        .findNodes(
                                Label.label(token),
                                propertyKey1,
                                propertyValue1,
                                propertyKey2,
                                propertyValue2,
                                propertyKey3,
                                propertyValue3);
            }
        },

        multipleNodesMap {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return tx.findNodes(Label.label(token), Map.of(propertyKey, propertyValue));
            }

            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2) {
                return tx.findNodes(
                        Label.label(token), Map.of(propertyKey1, propertyValue1, propertyKey2, propertyValue2));
            }

            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2,
                    String propertyKey3,
                    Object propertyValue3) {
                return tx.findNodes(
                        Label.label(token),
                        Map.of(
                                propertyKey1,
                                propertyValue1,
                                propertyKey2,
                                propertyValue2,
                                propertyKey3,
                                propertyValue3));
            }
        },

        multipleNodesFromExecutionContextMap {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return executionContextTransaction(tx)
                        .findNodes(Label.label(token), Map.of(propertyKey, propertyValue));
            }

            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2) {
                return executionContextTransaction(tx)
                        .findNodes(
                                Label.label(token), Map.of(propertyKey1, propertyValue1, propertyKey2, propertyValue2));
            }

            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2,
                    String propertyKey3,
                    Object propertyValue3) {
                return executionContextTransaction(tx)
                        .findNodes(
                                Label.label(token),
                                Map.of(
                                        propertyKey1,
                                        propertyValue1,
                                        propertyKey2,
                                        propertyValue2,
                                        propertyKey3,
                                        propertyValue3));
            }
        },

        stringSearchNodes {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, String template, StringSearchMode searchMode) {
                return tx.findNodes(Label.label(token), propertyKey, template, searchMode);
            }
        },

        stringSearchNodesFromExecutionContext {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, String template, StringSearchMode searchMode) {
                return executionContextTransaction(tx).findNodes(Label.label(token), propertyKey, template, searchMode);
            }
        },

        singleRelationship {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return Iterators.asResourceIterator(singletonList(
                        tx.findRelationship(RelationshipType.withName(token), propertyKey, propertyValue)));
            }
        },

        singleRelationshipFromExecutionContext {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return Iterators.asResourceIterator(singletonList(executionContextTransaction(tx)
                        .findRelationship(RelationshipType.withName(token), propertyKey, propertyValue)));
            }
        },

        multipleRelationships {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return tx.findRelationships(RelationshipType.withName(token), propertyKey, propertyValue);
            }
        },

        multipleRelationshipsFromExecutionContext {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return executionContextTransaction(tx)
                        .findRelationships(RelationshipType.withName(token), propertyKey, propertyValue);
            }
        },

        multipleRelationshipsComposite2 {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2) {
                return tx.findRelationships(
                        RelationshipType.withName(token), propertyKey1, propertyValue1, propertyKey2, propertyValue2);
            }
        },

        multipleRelationshipsFromExecutionContextComposite2 {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2) {
                return executionContextTransaction(tx)
                        .findRelationships(
                                RelationshipType.withName(token),
                                propertyKey1,
                                propertyValue1,
                                propertyKey2,
                                propertyValue2);
            }
        },

        multipleRelationshipsComposite3 {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2,
                    String propertyKey3,
                    Object propertyValue3) {
                return tx.findRelationships(
                        RelationshipType.withName(token),
                        propertyKey1,
                        propertyValue1,
                        propertyKey2,
                        propertyValue2,
                        propertyKey3,
                        propertyValue3);
            }
        },

        multipleRelationshipsFromExecutionContextComposite3 {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2,
                    String propertyKey3,
                    Object propertyValue3) {
                return executionContextTransaction(tx)
                        .findRelationships(
                                RelationshipType.withName(token),
                                propertyKey1,
                                propertyValue1,
                                propertyKey2,
                                propertyValue2,
                                propertyKey3,
                                propertyValue3);
            }
        },

        multipleRelationshipsMap {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return tx.findRelationships(RelationshipType.withName(token), Map.of(propertyKey, propertyValue));
            }

            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2) {
                return tx.findRelationships(
                        RelationshipType.withName(token),
                        Map.of(propertyKey1, propertyValue1, propertyKey2, propertyValue2));
            }

            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2,
                    String propertyKey3,
                    Object propertyValue3) {
                return tx.findRelationships(
                        RelationshipType.withName(token),
                        Map.of(
                                propertyKey1,
                                propertyValue1,
                                propertyKey2,
                                propertyValue2,
                                propertyKey3,
                                propertyValue3));
            }
        },

        multipleRelationshipsFromExecutionContextMap {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, Object propertyValue) {
                return executionContextTransaction(tx)
                        .findRelationships(RelationshipType.withName(token), Map.of(propertyKey, propertyValue));
            }

            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2) {
                return executionContextTransaction(tx)
                        .findRelationships(
                                RelationshipType.withName(token),
                                Map.of(propertyKey1, propertyValue1, propertyKey2, propertyValue2));
            }

            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx,
                    String token,
                    String propertyKey1,
                    Object propertyValue1,
                    String propertyKey2,
                    Object propertyValue2,
                    String propertyKey3,
                    Object propertyValue3) {
                return executionContextTransaction(tx)
                        .findRelationships(
                                RelationshipType.withName(token),
                                Map.of(
                                        propertyKey1,
                                        propertyValue1,
                                        propertyKey2,
                                        propertyValue2,
                                        propertyKey3,
                                        propertyValue3));
            }
        },

        stringSearchRelationships {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, String template, StringSearchMode searchMode) {
                return tx.findRelationships(RelationshipType.withName(token), propertyKey, template, searchMode);
            }
        },

        stringSearchRelationshipsFromExecutionContext {
            @Override
            ResourceIterator<? extends Entity> find(
                    Transaction tx, String token, String propertyKey, String template, StringSearchMode searchMode) {
                return executionContextTransaction(tx)
                        .findRelationships(RelationshipType.withName(token), propertyKey, template, searchMode);
            }
        };

        ResourceIterator<? extends Entity> find(
                Transaction tx, String token, String propertyKey, Object propertyValue) {
            throw new UnsupportedOperationException("This FindMethod does not support single property query");
        }

        ResourceIterator<? extends Entity> find(
                Transaction tx,
                String token,
                String propertyKey1,
                Object propertyValue1,
                String propertyKey2,
                Object propertyValue2) {
            throw new UnsupportedOperationException(
                    "This FindMethod does not support composite query with 2 property keys");
        }

        ResourceIterator<? extends Entity> find(
                Transaction tx,
                String token,
                String propertyKey1,
                Object propertyValue1,
                String propertyKey2,
                Object propertyValue2,
                String propertyKey3,
                Object propertyValue3) {
            throw new UnsupportedOperationException(
                    "This FindMethod does not support composite query with 3 property keys");
        }

        ResourceIterator<? extends Entity> find(
                Transaction tx, String token, String propertyKey, String template, StringSearchMode searchMode) {
            throw new UnsupportedOperationException("This FindMethod does not support string search");
        }
    }

    private enum EntityCreator {
        NODE {
            @Override
            Entity createEntity(Transaction tx, String token) {
                return tx.createNode(Label.label(token));
            }

            @Override
            IndexDescriptor createIndex(Transaction tx, IndexType indexType, String token, String... propertyKeys) {
                Label label = Label.label(token);
                IndexCreator indexCreator = tx.schema().indexFor(label);
                indexCreator = onProperties(indexCreator, propertyKeys);
                IndexDefinition indexDefinition =
                        indexCreator.withIndexType(indexType).create();
                return ((IndexDefinitionImpl) indexDefinition).getIndexReference();
            }
        },
        RELATIONSHIP {
            @Override
            Entity createEntity(Transaction tx, String token) {
                return tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName(token));
            }

            @Override
            IndexDescriptor createIndex(Transaction tx, IndexType indexType, String token, String... propertyKeys) {
                RelationshipType type = RelationshipType.withName(token);
                IndexCreator indexCreator = tx.schema().indexFor(type);
                indexCreator = onProperties(indexCreator, propertyKeys);
                IndexDefinition indexDefinition =
                        indexCreator.withIndexType(indexType).create();
                return ((IndexDefinitionImpl) indexDefinition).getIndexReference();
            }
        };

        abstract Entity createEntity(Transaction tx, String token);

        abstract IndexDescriptor createIndex(Transaction tx, IndexType indexType, String token, String... propertyKeys);

        private static IndexCreator onProperties(IndexCreator indexCreator, String[] propertyKeys) {
            for (String propertyKey : propertyKeys) {
                indexCreator = indexCreator.on(propertyKey);
            }
            return indexCreator;
        }
    }

    private enum SupportedIndexType {
        RANGE(
                IndexType.RANGE,
                Predicates.alwaysTrue(),
                // Suffix and contains not supported natively in the index but core API can use it and do the filtering
                Predicates.alwaysTrue(),
                true),
        TEXT(
                IndexType.TEXT,
                value -> value instanceof String || value instanceof Character,
                Predicates.alwaysTrue(),
                false),
        POINT(IndexType.POINT, value -> value instanceof Point, Predicates.alwaysFalse(), false),
        FULLTEXT(IndexType.FULLTEXT, Predicates.alwaysFalse(), Predicates.alwaysFalse(), true);

        private final IndexType indexType;
        private final Predicate<Object> supportedType;
        private final Predicate<StringSearchMode> supportedStringSearch;
        private final boolean supportCompositeIndex;

        SupportedIndexType(
                IndexType indexType,
                Predicate<Object> supportedType,
                Predicate<StringSearchMode> supportedStringSearch,
                boolean supportCompositeIndex) {
            this.indexType = indexType;
            this.supportedType = supportedType;
            this.supportedStringSearch = supportedStringSearch;
            this.supportCompositeIndex = supportCompositeIndex;
        }

        IndexType indexType() {
            return indexType;
        }

        boolean supportedType(Object... values) {
            return stream(values).allMatch(supportedType);
        }

        boolean supportedStringSearch(StringSearchMode searchMode) {
            return supportedStringSearch.test(searchMode);
        }

        boolean supportCompositeIndex() {
            return supportCompositeIndex;
        }
    }

    private enum SearchMode {
        EXACT(StringSearchMode.EXACT) {
            @Override
            String asTemplate(String propertyValue) {
                return propertyValue;
            }
        },
        PREFIX(StringSearchMode.PREFIX) {
            @Override
            String asTemplate(String propertyValue) {
                return propertyValue.substring(0, propertyValue.length() / 2);
            }
        },
        SUFFIX(StringSearchMode.SUFFIX) {
            @Override
            String asTemplate(String propertyValue) {
                return propertyValue.substring(propertyValue.length() / 2);
            }
        },
        CONTAINS(StringSearchMode.CONTAINS) {
            @Override
            String asTemplate(String propertyValue) {
                int quarter = propertyValue.length() / 4;
                return propertyValue.substring(quarter, quarter * 3);
            }
        };

        private final StringSearchMode mode;

        SearchMode(StringSearchMode mode) {
            this.mode = mode;
        }

        StringSearchMode mode() {
            return mode;
        }

        abstract String asTemplate(String propertyValue);
    }

    private static class MyIndexMonitor extends IndexMonitor.MonitorAdapter {
        private IndexDescriptor descriptor;
        private boolean queriedIndex;

        @Override
        public void queried(IndexDescriptor descriptor) {
            queriedIndex = true;
            this.descriptor = descriptor;
        }

        private void clear() {
            descriptor = null;
            queriedIndex = false;
        }
    }
}
