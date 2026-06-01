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
package org.neo4j.internal.schema;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.DEFAULT_TEXT_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.DEFAULT_VECTOR_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.FULLTEXT_V2_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.POINT_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.RANGE_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.TOKEN_DESCRIPTOR;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeKey;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodePropertyType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeUniqueness;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipKey;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipPropertyType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipUniqueness;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeFulltext;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeLookup;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodePoint;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeRange;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeText;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeVector;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipFulltext;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipLookup;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipPoint;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipRange;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipText;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipVector;
import org.neo4j.internal.schema.constraints.ExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.token.RegisteringCreatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Values;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@RandomSupportExtension
class SchemaCommandTest {

    private static final String NAME = "testing";
    // not used in conversion - only in index/constraint creation phase
    private static final boolean IF_NOT_EXISTS = false;

    private static final String[] PROPERTIES = tokens(42, "property");
    private static final String[] LABELS = tokens(13, "Label");
    private static final String[] TYPES = tokens(7, "Rel");

    // these IndexConfigs should be the authoritative configs (the ones serialized)
    // this depends on the index version, this test assumes latest provider

    private static final List<IndexConfig> FULLTEXT_CONFIGS = List.of(
            IndexConfig.empty(),
            IndexConfig.with(Map.of("fulltext.eventually_consistent", Values.booleanValue(true))),
            IndexConfig.with(Map.of("fulltext.eventually_consistent", Values.booleanValue(false))));

    private static final List<IndexConfig> POINT_CONFIGS = List.of(
            IndexConfig.empty(),
            IndexConfig.with(Map.of("spatial.cartesian.min", Values.doubleArray(new double[] {0.0, 0.0}))),
            IndexConfig.with(Map.of(
                    "spatial.cartesian.min",
                    Values.doubleArray(new double[] {0.0, 0.0}),
                    "spatial.cartesian.max",
                    Values.doubleArray(new double[] {1.0, 1.0}))));

    private static final List<IndexConfig> VECTOR_CONFIGS = List.of(
            // no empty authoritative config, the default (minimum authoritative config):
            IndexConfig.with(Map.ofEntries(
                    entry("vector.similarity_function", Values.utf8Value("COSINE")),
                    entry("vector.quantization.enabled", Values.booleanValue(true)),
                    entry("vector.hnsw.m", Values.intValue(16)),
                    entry("vector.hnsw.ef_construction", Values.intValue(100)))),
            IndexConfig.with(Map.ofEntries(
                    entry("vector.dimensions", Values.intValue(1536)),
                    entry("vector.similarity_function", Values.utf8Value("COSINE")),
                    entry("vector.quantization.enabled", Values.booleanValue(true)),
                    entry("vector.hnsw.m", Values.intValue(16)),
                    entry("vector.hnsw.ef_construction", Values.intValue(100)))),
            IndexConfig.with(Map.ofEntries(
                    entry("vector.dimensions", Values.intValue(768)),
                    entry("vector.similarity_function", Values.utf8Value("EUCLIDEAN")),
                    entry("vector.quantization.enabled", Values.booleanValue(false)),
                    entry("vector.hnsw.m", Values.intValue(32)),
                    entry("vector.hnsw.ef_construction", Values.intValue(200)))));

    private static final PropertyTypeSet[] PROPERTY_TYPES = new PropertyTypeSet[] {
        PropertyTypeSet.of(SchemaValueType.BOOLEAN),
        PropertyTypeSet.of(SchemaValueType.STRING),
        PropertyTypeSet.of(SchemaValueType.INTEGER),
        PropertyTypeSet.of(SchemaValueType.FLOAT),
        PropertyTypeSet.of(SchemaValueType.DATE),
        PropertyTypeSet.of(SchemaValueType.LOCAL_TIME),
        PropertyTypeSet.of(SchemaValueType.ZONED_TIME),
        PropertyTypeSet.of(SchemaValueType.LOCAL_DATETIME),
        PropertyTypeSet.of(SchemaValueType.ZONED_DATETIME),
        PropertyTypeSet.of(SchemaValueType.DURATION),
        PropertyTypeSet.of(SchemaValueType.POINT),
        PropertyTypeSet.of(SchemaValueType.LIST_BOOLEAN),
        PropertyTypeSet.of(SchemaValueType.LIST_STRING),
        PropertyTypeSet.of(SchemaValueType.LIST_INTEGER),
        PropertyTypeSet.of(SchemaValueType.LIST_FLOAT),
        PropertyTypeSet.of(SchemaValueType.LIST_LOCAL_TIME),
        PropertyTypeSet.of(SchemaValueType.LIST_ZONED_TIME),
        PropertyTypeSet.of(SchemaValueType.LIST_LOCAL_DATETIME),
        PropertyTypeSet.of(SchemaValueType.LIST_ZONED_DATETIME),
        PropertyTypeSet.of(SchemaValueType.LIST_DURATION),
        PropertyTypeSet.of(SchemaValueType.LIST_POINT),
        PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.FLOAT),
        PropertyTypeSet.of(SchemaValueType.INTEGER, SchemaValueType.FLOAT, SchemaValueType.STRING)
    };

    @Inject
    private RandomSupport random;

    private TokenHolders tokenHolders;

    @BeforeEach
    void setup() throws Exception {
        tokenHolders = new TokenHolders(
                createTokens(TYPE_PROPERTY_KEY, PROPERTIES),
                createTokens(TYPE_LABEL, LABELS),
                createTokens(TYPE_RELATIONSHIP_TYPE, TYPES));
    }

    @Test
    void indexTypesSanityCheck() {
        assertThat(Set.copyOf(AllIndexProviderDescriptors.INDEX_TYPES.values()))
                .as("""
                === Sanity check that we are not missing any IndexType(s) ===
                Different versions of the IndexProviderDescriptor should be covered in the SchemaCommandConverter which
                would catch changes to the commands themselves. This check is to ensure we don't miss any new IndexType(s)
                which would require new commands to be added to org.neo4j.internal.schema.SchemaCommand sealed type. The
                convertor would then need to be updated to populate this nwe command once the Cypher grammar has been updated.
                """)
                .containsExactlyInAnyOrder(IndexType.values());
    }

    @ParameterizedTest
    @MethodSource("names")
    void createLookupNode(String name) {
        assertThat(new NodeLookup(name, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), TOKEN_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.LOOKUP);
                    assertSchema(p.schema(), EntityType.NODE, List.of(), List.of());
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createLookupRelationship(String name) {
        assertThat(new RelationshipLookup(name, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), TOKEN_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.LOOKUP);
                    assertSchema(p.schema(), EntityType.RELATIONSHIP, List.of(), List.of());
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createRangeNode(String name) {
        String label = random.among(LABELS);
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));
        assertThat(new NodeRange(name, label, properties, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), RANGE_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.RANGE);
                    assertSchema(p.schema(), EntityType.NODE, List.of(label), properties);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createRangeRelationship(String name) {
        String type = random.among(TYPES);
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));
        assertThat(new RelationshipRange(name, type, properties, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), RANGE_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.RANGE);
                    assertSchema(p.schema(), EntityType.RELATIONSHIP, List.of(type), properties);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createTextNode(String name) {
        String label = random.among(LABELS);
        String property = random.among(PROPERTIES);
        assertThat(new NodeText(name, label, property, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), DEFAULT_TEXT_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.TEXT);
                    assertSchema(p.schema(), EntityType.NODE, List.of(label), List.of(property));
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createTextRelationship(String name) {
        String type = random.among(TYPES);
        String property = random.among(PROPERTIES);
        assertThat(new RelationshipText(name, type, property, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), DEFAULT_TEXT_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.TEXT);
                    assertSchema(p.schema(), EntityType.RELATIONSHIP, List.of(type), List.of(property));
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createPointNode(String name) {
        String label = random.among(LABELS);
        String property = random.among(PROPERTIES);
        IndexConfig config = random.among(POINT_CONFIGS);

        assertThat(new NodePoint(name, label, property, IF_NOT_EXISTS, config).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), POINT_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.POINT);
                    assertSchema(p.schema(), EntityType.NODE, List.of(label), List.of(property));
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createPointRelationship(String name) {
        String type = random.among(TYPES);
        String property = random.among(PROPERTIES);
        IndexConfig config = random.among(POINT_CONFIGS);

        assertThat(new RelationshipPoint(name, type, property, IF_NOT_EXISTS, config).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), POINT_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.POINT);
                    assertSchema(p.schema(), EntityType.RELATIONSHIP, List.of(type), List.of(property));
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createFulltextNode(String name) {
        List<String> labels = listFrom(LABELS, random.nextInt(1, 3));
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));
        IndexConfig config = random.among(FULLTEXT_CONFIGS);

        assertThat(new NodeFulltext(name, labels, properties, IF_NOT_EXISTS, config).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), FULLTEXT_V2_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.FULLTEXT);
                    assertSchema(p.schema(), EntityType.NODE, labels, properties);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createFulltextNodeV2(String name) {
        List<String> labels = listFrom(LABELS, random.nextInt(1, 3));
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));
        IndexConfig config = random.among(FULLTEXT_CONFIGS);

        assertThat(new NodeFulltext(name, labels, properties, IF_NOT_EXISTS, config).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), FULLTEXT_V2_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.FULLTEXT);
                    assertSchema(p.schema(), EntityType.NODE, labels, properties);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createFulltextRelationship(String name) {
        List<String> types = listFrom(TYPES, random.nextInt(1, 3));
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));
        IndexConfig config = random.among(FULLTEXT_CONFIGS);

        assertThat(new RelationshipFulltext(name, types, properties, IF_NOT_EXISTS, config).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), FULLTEXT_V2_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.FULLTEXT);
                    assertSchema(p.schema(), EntityType.RELATIONSHIP, types, properties);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createFulltextRelationshipV2(String name) {
        List<String> types = listFrom(TYPES, random.nextInt(1, 3));
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));
        IndexConfig config = random.among(FULLTEXT_CONFIGS);

        assertThat(new RelationshipFulltext(name, types, properties, IF_NOT_EXISTS, config).toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), FULLTEXT_V2_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.FULLTEXT);
                    assertSchema(p.schema(), EntityType.RELATIONSHIP, types, properties);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createVectorNode(String name) {
        String label = random.among(LABELS);
        String property = random.among(PROPERTIES);
        IndexConfig config = random.among(VECTOR_CONFIGS);

        assertThat(new NodeVector(
                                name,
                                List.of(label),
                                property,
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                IF_NOT_EXISTS,
                                config)
                        .toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), DEFAULT_VECTOR_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.VECTOR);
                    assertSchema(p.schema(), EntityType.NODE, List.of(label), List.of(property));
                    assertThat(p.schema().schemaPatternMatchingType())
                            .as("vector indexes should use PARTIAL_ANY_TOKEN")
                            .isEqualTo(SchemaPatternMatchingType.PARTIAL_ANY_TOKEN);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createVectorRelationship(String name) {
        String type = random.among(TYPES);
        String property = random.among(PROPERTIES);
        IndexConfig config = random.among(VECTOR_CONFIGS);

        assertThat(new RelationshipVector(
                                name,
                                List.of(type),
                                property,
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                IF_NOT_EXISTS,
                                config)
                        .toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), DEFAULT_VECTOR_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.VECTOR);
                    assertSchema(p.schema(), EntityType.RELATIONSHIP, List.of(type), List.of(property));
                    assertThat(p.schema().schemaPatternMatchingType())
                            .as("vector indexes should use PARTIAL_ANY_TOKEN")
                            .isEqualTo(SchemaPatternMatchingType.PARTIAL_ANY_TOKEN);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createVectorNodeWithMultipleLabelsAndAdditionalProperties(String name) {
        final var labels = listFrom(LABELS, random.nextInt(1, 3));
        final var vectorProperty = random.among(PROPERTIES);
        final var additionalProperties = listFrom(PROPERTIES, random.nextInt(1, 3));
        final var config = random.among(VECTOR_CONFIGS);

        assertThat(new NodeVector(
                                name,
                                labels,
                                vectorProperty,
                                additionalProperties,
                                DEFAULT_VECTOR_DESCRIPTOR,
                                IF_NOT_EXISTS,
                                config)
                        .toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), DEFAULT_VECTOR_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.VECTOR);
                    final var allProperties = Stream.concat(Stream.of(vectorProperty), additionalProperties.stream())
                            .toList();
                    assertSchema(p.schema(), EntityType.NODE, labels, allProperties);
                    assertThat(p.schema().schemaPatternMatchingType())
                            .as("multi-label vector indexes should use PARTIAL_ANY_TOKEN")
                            .isEqualTo(SchemaPatternMatchingType.PARTIAL_ANY_TOKEN);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void createVectorRelationshipWithMultipleTypesAndAdditionalProperties(String name) {
        final var types = listFrom(TYPES, random.nextInt(1, 3));
        final var vectorProperty = random.among(PROPERTIES);
        final var additionalProperties = listFrom(PROPERTIES, random.nextInt(1, 3));
        final var config = random.among(VECTOR_CONFIGS);

        assertThat(new RelationshipVector(
                                name,
                                types,
                                vectorProperty,
                                additionalProperties,
                                DEFAULT_VECTOR_DESCRIPTOR,
                                IF_NOT_EXISTS,
                                config)
                        .toPrototype(tokenHolders))
                .satisfies(p -> {
                    assertIndexName(p.getName(), name);
                    assertIndexProviderDescriptor(p.getIndexProvider(), DEFAULT_VECTOR_DESCRIPTOR);
                    assertThat(p.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.VECTOR);
                    final var allProperties = Stream.concat(Stream.of(vectorProperty), additionalProperties.stream())
                            .toList();
                    assertSchema(p.schema(), EntityType.RELATIONSHIP, types, allProperties);
                    assertThat(p.schema().schemaPatternMatchingType())
                            .as("multi-rel-type vector indexes should use PARTIAL_ANY_TOKEN")
                            .isEqualTo(SchemaPatternMatchingType.PARTIAL_ANY_TOKEN);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void nodeUniqueness(String name) {
        String label = random.among(LABELS);
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));

        assertThat(new NodeUniqueness(name, label, properties, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(UniquenessConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.UNDESIGNATED);
                    assertSchema(constraint.schema(), EntityType.NODE, List.of(label), properties);

                    IndexPrototype backingIndex = prototype.backingIndex();
                    assertThat(backingIndex)
                            .as("should return the backing index")
                            .isNotNull();
                    assertIndexName(backingIndex.getName(), constraint.getName());
                    assertIndexProviderDescriptor(backingIndex.getIndexProvider(), RANGE_DESCRIPTOR);
                    assertThat(backingIndex.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.RANGE);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void relationshipUniqueness(String name) {
        String type = random.among(TYPES);
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));

        assertThat(new RelationshipUniqueness(name, type, properties, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(UniquenessConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.UNDESIGNATED);
                    assertSchema(constraint.schema(), EntityType.RELATIONSHIP, List.of(type), properties);

                    IndexPrototype backingIndex = prototype.backingIndex();
                    assertThat(backingIndex)
                            .as("should return the backing index")
                            .isNotNull();
                    assertIndexName(backingIndex.getName(), constraint.getName());
                    assertIndexProviderDescriptor(backingIndex.getIndexProvider(), RANGE_DESCRIPTOR);
                    assertThat(backingIndex.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.RANGE);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void nodeKey(String name) {
        String label = random.among(LABELS);
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));

        assertThat(new NodeKey(name, label, properties, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(KeyConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.UNDESIGNATED);
                    assertSchema(constraint.schema(), EntityType.NODE, List.of(label), properties);

                    IndexPrototype backingIndex = prototype.backingIndex();
                    assertThat(backingIndex)
                            .as("should return the backing index")
                            .isNotNull();
                    assertIndexName(backingIndex.getName(), constraint.getName());
                    assertIndexProviderDescriptor(backingIndex.getIndexProvider(), RANGE_DESCRIPTOR);
                    assertThat(backingIndex.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.RANGE);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void relationshipKey(String name) {
        String type = random.among(TYPES);
        List<String> properties = listFrom(PROPERTIES, random.nextInt(1, 5));

        assertThat(new RelationshipKey(name, type, properties, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(KeyConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.UNDESIGNATED);
                    assertSchema(constraint.schema(), EntityType.RELATIONSHIP, List.of(type), properties);

                    IndexPrototype backingIndex = prototype.backingIndex();
                    assertThat(backingIndex)
                            .as("should return the backing index")
                            .isNotNull();
                    assertIndexName(backingIndex.getName(), constraint.getName());
                    assertIndexProviderDescriptor(backingIndex.getIndexProvider(), RANGE_DESCRIPTOR);
                    assertThat(backingIndex.getIndexType())
                            .as("should have the correct index type")
                            .isEqualTo(IndexType.RANGE);
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void nodeExistence(String name) {
        String label = random.among(LABELS);
        String property = random.among(PROPERTIES);

        assertThat(new NodeExistence(name, label, property, false, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(ExistenceConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.INDEPENDENT);
                    assertSchema(constraint.schema(), EntityType.NODE, List.of(label), List.of(property));

                    assertThat(prototype.backingIndex())
                            .as("should have no backing index")
                            .isNull();
                });
        assertThat(new NodeExistence(name, label, property, true, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(ExistenceConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.DEPENDENT);
                    assertSchema(constraint.schema(), EntityType.NODE, List.of(label), List.of(property));

                    assertThat(prototype.backingIndex())
                            .as("should have no backing index")
                            .isNull();
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void relationshipExistence(String name) {
        String type = random.among(TYPES);
        String property = random.among(PROPERTIES);

        assertThat(new RelationshipExistence(name, type, property, false, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(ExistenceConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.INDEPENDENT);
                    assertSchema(constraint.schema(), EntityType.RELATIONSHIP, List.of(type), List.of(property));

                    assertThat(prototype.backingIndex())
                            .as("should have no backing index")
                            .isNull();
                });
        assertThat(new RelationshipExistence(name, type, property, true, IF_NOT_EXISTS).toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(ExistenceConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.DEPENDENT);
                    assertSchema(constraint.schema(), EntityType.RELATIONSHIP, List.of(type), List.of(property));

                    assertThat(prototype.backingIndex())
                            .as("should have no backing index")
                            .isNull();
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void nodePropertyType(String name) {
        String label = random.among(LABELS);
        String property = random.among(PROPERTIES);
        PropertyTypeSet propertyTypes = random.among(PROPERTY_TYPES);

        assertThat(new NodePropertyType(name, label, property, propertyTypes, false, IF_NOT_EXISTS)
                        .toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(TypeConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.INDEPENDENT);
                    assertThat(((TypeConstraintDescriptor) constraint).propertyType())
                            .isEqualTo(propertyTypes);
                    assertSchema(constraint.schema(), EntityType.NODE, List.of(label), List.of(property));

                    assertThat(prototype.backingIndex())
                            .as("should have no backing index")
                            .isNull();
                });
        assertThat(new NodePropertyType(name, label, property, propertyTypes, true, IF_NOT_EXISTS)
                        .toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(TypeConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.DEPENDENT);
                    assertThat(((TypeConstraintDescriptor) constraint).propertyType())
                            .isEqualTo(propertyTypes);
                    assertSchema(constraint.schema(), EntityType.NODE, List.of(label), List.of(property));

                    assertThat(prototype.backingIndex())
                            .as("should have no backing index")
                            .isNull();
                });
    }

    @ParameterizedTest
    @MethodSource("names")
    void relationshipPropertyType(String name) {
        String type = random.among(TYPES);
        String property = random.among(PROPERTIES);
        PropertyTypeSet propertyTypes = random.among(PROPERTY_TYPES);

        assertThat(new RelationshipPropertyType(name, type, property, propertyTypes, false, IF_NOT_EXISTS)
                        .toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(TypeConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.INDEPENDENT);
                    assertThat(((TypeConstraintDescriptor) constraint).propertyType())
                            .isEqualTo(propertyTypes);
                    assertSchema(constraint.schema(), EntityType.RELATIONSHIP, List.of(type), List.of(property));

                    assertThat(prototype.backingIndex())
                            .as("should have no backing index")
                            .isNull();
                });
        assertThat(new RelationshipPropertyType(name, type, property, propertyTypes, true, IF_NOT_EXISTS)
                        .toPrototype(tokenHolders))
                .satisfies(prototype -> {
                    ConstraintDescriptor constraint = prototype.descriptor();
                    assertThat(constraint).isInstanceOf(TypeConstraintDescriptor.class);
                    assertConstraintName(constraint.getName(), name);
                    assertThat(constraint.graphTypeDependence()).isEqualTo(GraphTypeDependence.DEPENDENT);
                    assertThat(((TypeConstraintDescriptor) constraint).propertyType())
                            .isEqualTo(propertyTypes);
                    assertSchema(constraint.schema(), EntityType.RELATIONSHIP, List.of(type), List.of(property));

                    assertThat(prototype.backingIndex())
                            .as("should have no backing index")
                            .isNull();
                });
    }

    @Test
    void schemaTokens() {
        int id = 1;
        MutableSet<String> labels = Sets.mutable.empty();
        MutableSet<String> relationships = Sets.mutable.empty();
        MutableSet<String> properties = Sets.mutable.empty();
        List<IndexCommand.Create> indexes = List.of(
                new NodeLookup("command" + id++, IF_NOT_EXISTS),
                new RelationshipLookup("command" + id++, IF_NOT_EXISTS),
                new NodeRange(
                        "command" + id++,
                        track(LABELS, labels, random),
                        trackAll(PROPERTIES, properties, random),
                        IF_NOT_EXISTS),
                new RelationshipRange(
                        "command" + id++,
                        track(TYPES, relationships, random),
                        trackAll(PROPERTIES, properties, random),
                        IF_NOT_EXISTS),
                new NodePoint(
                        "command" + id++,
                        track(LABELS, labels, random),
                        track(PROPERTIES, properties, random),
                        IF_NOT_EXISTS,
                        random.among(POINT_CONFIGS)),
                new RelationshipPoint(
                        "command" + id++,
                        track(TYPES, relationships, random),
                        track(PROPERTIES, properties, random),
                        IF_NOT_EXISTS,
                        random.among(POINT_CONFIGS)),
                new NodeText(
                        "command" + id++,
                        track(LABELS, labels, random),
                        track(PROPERTIES, properties, random),
                        IF_NOT_EXISTS),
                new RelationshipText(
                        "command" + id++,
                        track(TYPES, relationships, random),
                        track(PROPERTIES, properties, random),
                        IF_NOT_EXISTS),
                new NodeFulltext(
                        "command" + id++,
                        trackAll(LABELS, labels, random),
                        trackAll(PROPERTIES, properties, random),
                        IF_NOT_EXISTS,
                        random.among(FULLTEXT_CONFIGS)),
                new RelationshipFulltext(
                        "command" + id++,
                        trackAll(TYPES, relationships, random),
                        trackAll(PROPERTIES, properties, random),
                        IF_NOT_EXISTS,
                        random.among(FULLTEXT_CONFIGS)),
                new NodeVector(
                        "command" + id++,
                        List.of(track(LABELS, labels, random)),
                        track(PROPERTIES, properties, random),
                        List.of(),
                        DEFAULT_VECTOR_DESCRIPTOR,
                        IF_NOT_EXISTS,
                        random.among(VECTOR_CONFIGS)),
                new RelationshipVector(
                        "command" + id++,
                        List.of(track(TYPES, relationships, random)),
                        track(PROPERTIES, properties, random),
                        List.of(),
                        DEFAULT_VECTOR_DESCRIPTOR,
                        IF_NOT_EXISTS,
                        random.among(VECTOR_CONFIGS)));
        List<ConstraintCommand.Create> constraints = List.of(
                new NodeKey(
                        "command" + id++,
                        track(LABELS, labels, random),
                        trackAll(PROPERTIES, properties, random),
                        IF_NOT_EXISTS),
                new RelationshipKey(
                        "command" + id++,
                        track(TYPES, relationships, random),
                        trackAll(PROPERTIES, properties, random),
                        IF_NOT_EXISTS),
                new NodeUniqueness(
                        "command" + id++,
                        track(LABELS, labels, random),
                        trackAll(PROPERTIES, properties, random),
                        IF_NOT_EXISTS),
                new RelationshipUniqueness(
                        "command" + id++,
                        track(TYPES, relationships, random),
                        trackAll(PROPERTIES, properties, random),
                        IF_NOT_EXISTS),
                new NodeExistence(
                        "command" + id++,
                        track(LABELS, labels, random),
                        track(PROPERTIES, properties, random),
                        false,
                        IF_NOT_EXISTS),
                new RelationshipExistence(
                        "command" + id++,
                        track(TYPES, relationships, random),
                        track(PROPERTIES, properties, random),
                        false,
                        IF_NOT_EXISTS),
                new NodePropertyType(
                        "command" + id++,
                        track(LABELS, labels, random),
                        track(PROPERTIES, properties, random),
                        random.among(PROPERTY_TYPES),
                        false,
                        IF_NOT_EXISTS),
                new RelationshipPropertyType(
                        "command" + id,
                        track(TYPES, relationships, random),
                        track(PROPERTIES, properties, random),
                        random.among(PROPERTY_TYPES),
                        false,
                        IF_NOT_EXISTS));
        SchemaTokens tokens = SchemaTokens.collect(indexes, constraints);
        assertThat(tokens.labels()).containsAll(labels);
        assertThat(tokens.relationships()).containsAll(relationships);
        assertThat(tokens.properties()).containsAll(properties);
    }

    private static String track(String[] options, MutableSet<String> tokens, RandomSupport random) {
        String option = random.among(options);
        tokens.add(option);
        return option;
    }

    private static List<String> trackAll(String[] options, MutableSet<String> tokens, RandomSupport random) {
        List<String> values = listFrom(options, random.nextInt(1, 4), random);
        tokens.addAll(values);
        return values;
    }

    private static void assertIndexName(Optional<String> indexName, String expected) {
        assertThat(indexName).as("should always have a name").isPresent();
        if (expected != null) {
            assertThat(indexName.orElseThrow())
                    .as("should use the provided name")
                    .isEqualTo(expected);
        }
    }

    private static void assertConstraintName(String constraintName, String expected) {
        if (expected != null) {
            assertThat(constraintName).as("should use the provided name").isEqualTo(expected);
        }
    }

    private static void assertIndexProviderDescriptor(
            IndexProviderDescriptor actual, IndexProviderDescriptor expected) {
        assertThat(actual).as("should have the expected index provider").isEqualTo(expected);
    }

    private void assertSchema(
            SchemaDescriptor schema, EntityType entityType, List<String> tokens, List<String> properties) {
        assertThat(schema.entityType())
                .as("should have the correct entity type")
                .isEqualTo(entityType);

        TokenHolder tokenHolder =
                entityType == EntityType.NODE ? tokenHolders.labelTokens() : tokenHolders.relationshipTypeTokens();
        MutableIntSet tokenIds = IntSets.mutable.of();
        for (String token : tokens) {
            tokenIds.add(tokenHolder.getIdByName(token));
        }
        assertThat(IntSets.mutable.with(schema.getEntityTokenIds()))
                .as("should have the correct entity tokens")
                .isEqualTo(tokenIds);

        MutableIntSet propertyIds = IntSets.mutable.of();
        for (String property : properties) {
            propertyIds.add(tokenHolders.propertyKeyTokens().getIdByName(property));
        }
        assertThat(IntSets.mutable.with(schema.getPropertyIds()))
                .as("should have the correct property tokens")
                .isEqualTo(propertyIds);
    }

    private <T> List<T> listFrom(T[] items, int count) {
        return listFrom(items, count, random);
    }

    private static <T> List<T> listFrom(T[] items, int count, RandomSupport random) {
        MutableSet<T> result = Sets.mutable.withInitialCapacity(count);
        while (result.size() < count) {
            result.add(random.among(items));
        }
        return result.toList();
    }

    private static TokenHolder createTokens(String type, String... tokens) throws Exception {
        RegisteringCreatingTokenHolder holder = new RegisteringCreatingTokenHolder(tokenCreator(), type);
        for (String token : tokens) {
            holder.getOrCreateId(token);
        }
        return holder;
    }

    private static TokenCreator tokenCreator() {
        AtomicInteger counter = new AtomicInteger();
        return (name, internal) -> counter.getAndIncrement();
    }

    private static Stream<String> names() {
        return Stream.of(null, NAME);
    }

    private static String[] tokens(int max, String prefix) {
        return IntStream.range(0, max).mapToObj(i -> prefix + i).toArray(String[]::new);
    }
}
