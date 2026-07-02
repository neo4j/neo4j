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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DEFAULT_SEARCH_EXPANSION_FACTOR;
import static org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DIMENSIONS;
import static org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_HNSW_EF_CONSTRUCTION;
import static org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_HNSW_M;
import static org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_QUANTIZATION_TYPE;
import static org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_SIMILARITY_FUNCTION;
import static org.neo4j.internal.schema.IndexType.FULLTEXT;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.internal.schema.IndexType.POINT;
import static org.neo4j.internal.schema.IndexType.VECTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.FulltextSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.SettingsAccessor.IndexConfigAccessor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.fulltext.FulltextIndexSettingsKeys;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.schema.SchemaTestUtil;
import org.neo4j.kernel.impl.index.schema.SpatialIndexConfig;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.test.LatestVersions;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SchemaStatementCreatorTest {
    private static final String NAME_WITH_BACKTICKS = "``ugly`name`";
    private static final String ESCAPED_NAME_WITH_BACKTICKS = "`````ugly``name```";
    private static final String LABEL_WITH_BACKTICKS = "```label";
    private static final String ESCAPED_LABEL_WITH_BACKTICKS = "```````label`";
    private static final String TYPE_WITH_BACKTICKS = "`ty``pe";
    private static final String ESCAPED_TYPE_WITH_BACKTICKS = "```ty````pe`";
    private static final String PROPERTY_KEY_WITH_BACKTICKS = "prop`";
    private static final String ESCAPED_PROPERTY_KEY_WITH_BACKTICKS = "`prop```";
    // Id to use if test for escaping of label/type/property
    private static final int ESC_BACKTICK_ID = 10;

    TokenRead tokenRead;

    @BeforeEach
    void setUp() throws KernelException {
        tokenRead = Mockito.mock(TokenRead.class);
        when(tokenRead.nodeLabelName(anyInt()))
                .thenAnswer(i -> SchemaTestUtil.SIMPLE_NAME_LOOKUP.labelGetName(i.getArgument(0)));
        when(tokenRead.relationshipTypeName(anyInt()))
                .thenAnswer(i -> SchemaTestUtil.SIMPLE_NAME_LOOKUP.relationshipTypeGetName(i.getArgument(0)));
        when(tokenRead.propertyKeyName(anyInt()))
                .thenAnswer(i -> SchemaTestUtil.SIMPLE_NAME_LOOKUP.propertyKeyGetName(i.getArgument(0)));

        when(tokenRead.nodeLabelName(ESC_BACKTICK_ID)).thenReturn(LABEL_WITH_BACKTICKS);
        when(tokenRead.relationshipTypeName(ESC_BACKTICK_ID)).thenReturn(TYPE_WITH_BACKTICKS);
        when(tokenRead.propertyKeyName(ESC_BACKTICK_ID)).thenReturn(PROPERTY_KEY_WITH_BACKTICKS);
    }

    @Test
    void canGenerateSchemaStatementForTokenIndex() throws KernelException {
        IndexDescriptor labelTokenIndex = IndexPrototype.forSchema(
                        ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, AllIndexProviderDescriptors.TOKEN_DESCRIPTOR)
                .withIndexType(LOOKUP)
                .withName("labelTokenIndex")
                .materialise(1);
        IndexDescriptor typeTokenIndex = IndexPrototype.forSchema(
                        ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR, AllIndexProviderDescriptors.TOKEN_DESCRIPTOR)
                .withIndexType(LOOKUP)
                .withName("typeTokenIndex")
                .materialise(2);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, labelTokenIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, typeTokenIndex);

        assertThat(labelIndex).isEqualTo("CREATE LOOKUP INDEX `labelTokenIndex` FOR (n) ON EACH labels(n)");
        assertThat(typeIndex).isEqualTo("CREATE LOOKUP INDEX `typeTokenIndex` FOR ()-[r]-() ON EACH type(r)");
    }

    @Test
    void canGenerateSchemaStatementForDefaultIndex() throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(
                        forLabel(0, 1), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("nodeIndex")
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(
                        forRelType(0, 1), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("relIndex")
                .materialise(2);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo("CREATE RANGE INDEX `nodeIndex` FOR (n:`Label0`) ON (n.`property1`) OPTIONS {}");
        assertThat(typeIndex)
                .isEqualTo("CREATE RANGE INDEX `relIndex` FOR ()-[r:`RelType0`]-() ON (r.`property1`) OPTIONS {}");
    }

    @EnumSource(value = PropertyIndex.class)
    @ParameterizedTest
    void canGenerateSchemaStatementForPropertyIndex(PropertyIndex index) throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(forLabel(0, 1), index.descriptor)
                .withName("nodeIndex")
                .withIndexType(index.indexType)
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(forRelType(0, 1), index.descriptor)
                .withName("relIndex")
                .withIndexType(index.indexType)
                .materialise(2);

        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo("CREATE " + index.indexType
                        + " INDEX `nodeIndex` FOR (n:`Label0`) ON (n.`property1`) OPTIONS {}");
        assertThat(typeIndex)
                .isEqualTo("CREATE " + index.indexType
                        + " INDEX `relIndex` FOR ()-[r:`RelType0`]-() ON (r.`property1`) OPTIONS {}");
    }

    @EnumSource(value = PropertyIndex.class)
    @ParameterizedTest
    void canGenerateSchemaStatementForPropertyIndexWithIndexProvider(PropertyIndex index) throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(forLabel(0, 1), index.descriptor)
                .withName("nodeIndex")
                .withIndexType(index.indexType)
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(forRelType(0, 1), index.descriptor)
                .withName("relIndex")
                .withIndexType(index.indexType)
                .materialise(2);

        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex, true);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex, true);

        assertThat(labelIndex)
                .isEqualTo("CYPHER 5 CREATE " + index.indexType
                        + " INDEX `nodeIndex` FOR (n:`Label0`) ON (n.`property1`) OPTIONS {indexProvider: '"
                        + index.descriptor.name() + "'}");
        assertThat(typeIndex)
                .isEqualTo("CYPHER 5 CREATE " + index.indexType
                        + " INDEX `relIndex` FOR ()-[r:`RelType0`]-() ON (r.`property1`) OPTIONS {indexProvider: '"
                        + index.descriptor.name() + "'}");
    }

    @Test
    void canGenerateSchemaStatementForCompositeDefaultIndex() throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(
                        forLabel(0, 1, 2), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("nodeIndex")
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(
                        forRelType(0, 1, 2), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("relIndex")
                .materialise(2);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo(
                        "CREATE RANGE INDEX `nodeIndex` FOR (n:`Label0`) ON (n.`property1`, n.`property2`) OPTIONS {}");
        assertThat(typeIndex)
                .isEqualTo(
                        "CREATE RANGE INDEX `relIndex` FOR ()-[r:`RelType0`]-() ON (r.`property1`, r.`property2`) OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForPointIndexWithSpecialOptions() throws KernelException {
        IndexDescriptor nodeIndex =
                addIndexConfig(IndexPrototype.forSchema(forLabel(0, 1), AllIndexProviderDescriptors.POINT_DESCRIPTOR)
                        .withIndexType(POINT)
                        .withName("nodeIndex")
                        .materialise(1));
        IndexDescriptor relIndex =
                addIndexConfig(IndexPrototype.forSchema(forRelType(0, 1), AllIndexProviderDescriptors.POINT_DESCRIPTOR)
                        .withIndexType(POINT)
                        .withName("relIndex")
                        .materialise(2));

        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo(
                        "CREATE POINT INDEX `nodeIndex` FOR (n:`Label0`) ON (n.`property1`) OPTIONS "
                                + "{indexConfig: "
                                + "{`spatial.cartesian.max`: [1000000.0, 1000000.0], `spatial.cartesian.min`: [-1000000.0, -1000000.0]}}");
        assertThat(typeIndex)
                .isEqualTo(
                        "CREATE POINT INDEX `relIndex` FOR ()-[r:`RelType0`]-() ON (r.`property1`) OPTIONS "
                                + "{indexConfig: "
                                + "{`spatial.cartesian.max`: [1000000.0, 1000000.0], `spatial.cartesian.min`: [-1000000.0, -1000000.0]}}");
    }

    @Test
    void canGenerateSchemaStatementForFulltextIndex() throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(
                        SchemaDescriptors.forSemanticSearch(NODE, new int[] {0}, new int[] {1}),
                        AllIndexProviderDescriptors.FULLTEXT_V2_DESCRIPTOR)
                .withIndexType(FULLTEXT)
                .withName("nodeIndex")
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(
                        SchemaDescriptors.forSemanticSearch(RELATIONSHIP, new int[] {0}, new int[] {1}),
                        AllIndexProviderDescriptors.FULLTEXT_V1_DESCRIPTOR)
                .withIndexType(FULLTEXT)
                .withName("relIndex")
                .materialise(2);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo("CREATE FULLTEXT INDEX `nodeIndex` FOR (n:`Label0`) ON EACH [n.`property1`] OPTIONS {}");
        assertThat(typeIndex)
                .isEqualTo(
                        "CREATE FULLTEXT INDEX `relIndex` FOR ()-[r:`RelType0`]-() ON EACH [r.`property1`] OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForCompositeMultiTokenFulltextIndex() throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(
                        SchemaDescriptors.forSemanticSearch(NODE, new int[] {0, 1}, new int[] {2, 3}),
                        AllIndexProviderDescriptors.FULLTEXT_V2_DESCRIPTOR)
                .withIndexType(FULLTEXT)
                .withName("nodeIndex")
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(
                        SchemaDescriptors.forSemanticSearch(RELATIONSHIP, new int[] {0, 1}, new int[] {2, 3}),
                        AllIndexProviderDescriptors.FULLTEXT_V1_DESCRIPTOR)
                .withIndexType(FULLTEXT)
                .withName("relIndex")
                .materialise(2);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo(
                        "CREATE FULLTEXT INDEX `nodeIndex` FOR (n:`Label0`|`Label1`) ON EACH [n.`property2`, n.`property3`] "
                                + "OPTIONS {}");
        assertThat(typeIndex)
                .isEqualTo(
                        "CREATE FULLTEXT INDEX `relIndex` FOR ()-[r:`RelType0`|`RelType1`]-() ON EACH [r.`property2`, r.`property3`] "
                                + "OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForFulltextIndexWithConfig() throws KernelException {
        IndexConfig config = IndexConfig.with(
                        FulltextIndexSettingsKeys.ANALYZER,
                        Values.stringValue(FulltextSettings.fulltext_default_analyzer.defaultValue()))
                .withIfAbsent(FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT, Values.booleanValue(true));

        IndexDescriptor nodeIndex = IndexPrototype.forSchema(
                        SchemaDescriptors.forSemanticSearch(NODE, new int[] {0, 1}, new int[] {2, 3}),
                        AllIndexProviderDescriptors.FULLTEXT_V1_DESCRIPTOR)
                .withIndexType(FULLTEXT)
                .withName("nodeIndex")
                .withIndexConfig(config)
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(
                        SchemaDescriptors.forSemanticSearch(RELATIONSHIP, new int[] {0, 1}, new int[] {2, 3}),
                        AllIndexProviderDescriptors.FULLTEXT_V2_DESCRIPTOR)
                .withIndexType(FULLTEXT)
                .withName("relIndex")
                .withIndexConfig(config)
                .materialise(2);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo(
                        "CREATE FULLTEXT INDEX `nodeIndex` FOR (n:`Label0`|`Label1`) ON EACH [n.`property2`, n.`property3`] "
                                + "OPTIONS {indexConfig: {`fulltext.analyzer`: 'standard-no-stop-words', "
                                + "`fulltext.eventually_consistent`: true}}");
        assertThat(typeIndex)
                .isEqualTo(
                        "CREATE FULLTEXT INDEX `relIndex` FOR ()-[r:`RelType0`|`RelType1`]-() ON EACH [r.`property2`, r.`property3`] "
                                + "OPTIONS {indexConfig: {`fulltext.analyzer`: 'standard-no-stop-words', "
                                + "`fulltext.eventually_consistent`: true}}");
    }

    @Test
    void canGenerateSchemaStatementForVectorIndex() throws KernelException {
        KernelVersion kernelVersion = LatestVersions.LATEST_KERNEL_VERSION;
        VectorIndexVersion version = VectorIndexVersion.latestSupportedVersion(kernelVersion);
        IndexConfig configFromUser = IndexSettingUtil.defaultConfigForTest(VECTOR.toPublicApi());
        VectorIndexConfig vectorIndexConfig = version.indexSettingValidator(kernelVersion)
                .validateToTypedConfig(new IndexConfigAccessor(configFromUser));

        IndexPrototype prototype = IndexPrototype.forSchema(forLabel(0, 1), version.descriptor())
                .withIndexType(VECTOR)
                .withName("nodeIndex")
                .withIndexConfig(configFromUser);
        // simulate VectorIndexProvider::validateIndexPrototype
        prototype = prototype.withIndexConfig(vectorIndexConfig.config());

        IndexDescriptor nodeIndex = prototype.materialise(1);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);

        assertThat(labelIndex).isEqualToIgnoringWhitespace("""
                        CREATE VECTOR INDEX `nodeIndex`
                        FOR (n:`Label0`) ON (n.`property1`)
                        OPTIONS {
                            indexConfig: {
                                `%s`: %s,
                                `%s`: %s,
                                `%s`: %s,
                                `%s`: %s,
                                `%s`: '%s',
                                `%s`: '%s'
                            }
                        }
                        """.formatted(
                        VECTOR_DEFAULT_SEARCH_EXPANSION_FACTOR.getSettingName(),
                        Double.toString(vectorIndexConfig.defaultSearchExpansionFactor()),
                        VECTOR_DIMENSIONS.getSettingName(),
                        Integer.toString(vectorIndexConfig.dimensions().orElseThrow()),
                        VECTOR_HNSW_EF_CONSTRUCTION.getSettingName(),
                        Integer.toString(vectorIndexConfig.hnsw().efConstruction()),
                        VECTOR_HNSW_M.getSettingName(),
                        Integer.toString(vectorIndexConfig.hnsw().M()),
                        VECTOR_QUANTIZATION_TYPE.getSettingName(),
                        vectorIndexConfig.quantization().name(),
                        VECTOR_SIMILARITY_FUNCTION.getSettingName(),
                        vectorIndexConfig.similarityFunction().functionName()));
    }

    @Test
    void canGenerateSchemaStatementForNodeKeyConstraint() throws KernelException {
        ConstraintDescriptor nodeConstraint =
                ConstraintDescriptorFactory.nodeKeyForLabel(1, 2).withName("nodeConstraint");
        IndexDescriptor nodeIndex = IndexPrototype.uniqueForSchema(
                        forLabel(1, 2), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("nodeIndex")
                .materialise(1);

        String nodeKeyConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> nodeIndex, tokenRead, nodeConstraint);

        assertThat(nodeKeyConstraint)
                .isEqualTo("CREATE CONSTRAINT `nodeConstraint` FOR (n:`Label1`) REQUIRE (n.`property2`) "
                        + "IS NODE KEY OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForCompositeNodeKeyConstraint() throws KernelException {
        ConstraintDescriptor nodeConstraint =
                ConstraintDescriptorFactory.nodeKeyForLabel(1, 2, 3).withName("nodeConstraint");
        IndexDescriptor nodeIndex = IndexPrototype.uniqueForSchema(
                        forLabel(1, 2, 3), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("nodeIndex")
                .materialise(1);

        String nodeKeyConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> nodeIndex, tokenRead, nodeConstraint);

        assertThat(nodeKeyConstraint)
                .isEqualTo("CREATE CONSTRAINT `nodeConstraint` FOR (n:`Label1`) REQUIRE (n.`property2`, n.`property3`) "
                        + "IS NODE KEY OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForRelKeyConstraint() throws KernelException {
        ConstraintDescriptor relConstraint = ConstraintDescriptorFactory.keyForSchema(
                        SchemaDescriptors.forRelType(1, 2))
                .withName("relConstraint");
        IndexDescriptor relIndex = IndexPrototype.uniqueForSchema(
                        forRelType(1, 2), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("relIndex")
                .materialise(1);

        String relKeyConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> relIndex, tokenRead, relConstraint);

        assertThat(relKeyConstraint)
                .isEqualTo("CREATE CONSTRAINT `relConstraint` FOR ()-[r:`RelType1`]-() REQUIRE (r.`property2`) "
                        + "IS RELATIONSHIP KEY OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForCompositeRelKeyConstraint() throws KernelException {
        ConstraintDescriptor relConstraint = ConstraintDescriptorFactory.keyForSchema(
                        SchemaDescriptors.forRelType(1, 2, 3))
                .withName("relConstraint");
        IndexDescriptor relIndex = IndexPrototype.uniqueForSchema(
                        forRelType(1, 2, 3), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("relIndex")
                .materialise(1);

        String relKeyConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> relIndex, tokenRead, relConstraint);

        assertThat(relKeyConstraint)
                .isEqualTo(
                        "CREATE CONSTRAINT `relConstraint` FOR ()-[r:`RelType1`]-() REQUIRE (r.`property2`, r.`property3`) "
                                + "IS RELATIONSHIP KEY OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForNodeUniquenessConstraint() throws KernelException {
        ConstraintDescriptor nodeConstraint =
                ConstraintDescriptorFactory.uniqueForLabel(1, 2).withName("nodeConstraint");
        IndexDescriptor nodeIndex = IndexPrototype.uniqueForSchema(
                        forLabel(1, 2), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("nodeIndex")
                .materialise(1);

        String uniqueConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> nodeIndex, tokenRead, nodeConstraint);

        assertThat(uniqueConstraint)
                .isEqualTo("CREATE CONSTRAINT `nodeConstraint` FOR (n:`Label1`) REQUIRE (n.`property2`) "
                        + "IS UNIQUE OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementAndEscapeName() throws KernelException {
        String constraintName = "node`Constraint";
        ConstraintDescriptor nodeConstraint =
                ConstraintDescriptorFactory.uniqueForLabel(1, 2).withName(constraintName);
        IndexDescriptor nodeIndex = IndexPrototype.uniqueForSchema(
                        forLabel(1, 2), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("nodeIndex")
                .materialise(1);
        Map<String, IndexDescriptor> indexes = new HashMap<>();
        indexes.put(constraintName, nodeIndex);

        String uniqueConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(indexes::get, tokenRead, nodeConstraint);

        assertThat(uniqueConstraint)
                .isEqualTo("CREATE CONSTRAINT `node``Constraint` FOR (n:`Label1`) REQUIRE (n.`property2`) "
                        + "IS UNIQUE OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForCompositeNodeUniquenessConstraint() throws KernelException {
        ConstraintDescriptor nodeConstraint =
                ConstraintDescriptorFactory.uniqueForLabel(1, 2, 3).withName("nodeConstraint");
        IndexDescriptor nodeIndex = IndexPrototype.uniqueForSchema(
                        forLabel(1, 2, 3), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("nodeIndex")
                .materialise(1);

        String uniqueConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> nodeIndex, tokenRead, nodeConstraint);

        assertThat(uniqueConstraint)
                .isEqualTo("CREATE CONSTRAINT `nodeConstraint` FOR (n:`Label1`) REQUIRE (n.`property2`, n.`property3`) "
                        + "IS UNIQUE OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForRelUniquenessConstraint() throws KernelException {
        ConstraintDescriptor relConstraint = ConstraintDescriptorFactory.uniqueForSchema(
                        SchemaDescriptors.forRelType(1, 2))
                .withName("relConstraint");
        IndexDescriptor relIndex = IndexPrototype.uniqueForSchema(
                        forRelType(1, 2), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("relIndex")
                .materialise(1);

        String uniqueConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> relIndex, tokenRead, relConstraint);

        assertThat(uniqueConstraint)
                .isEqualTo("CREATE CONSTRAINT `relConstraint` FOR ()-[r:`RelType1`]-() REQUIRE (r.`property2`) "
                        + "IS UNIQUE OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForCompositeRelUniquenessConstraint() throws KernelException {
        ConstraintDescriptor relConstraint = ConstraintDescriptorFactory.uniqueForSchema(
                        SchemaDescriptors.forRelType(1, 2, 3))
                .withName("relConstraint");
        IndexDescriptor relIndex = IndexPrototype.uniqueForSchema(
                        forRelType(1, 2, 3), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("relIndex")
                .materialise(1);

        String uniqueConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> relIndex, tokenRead, relConstraint);

        assertThat(uniqueConstraint)
                .isEqualTo(
                        "CREATE CONSTRAINT `relConstraint` FOR ()-[r:`RelType1`]-() REQUIRE (r.`property2`, r.`property3`) "
                                + "IS UNIQUE OPTIONS {}");
    }

    @Test
    void canGenerateSchemaStatementForExistenceConstraints() throws KernelException {
        ConstraintDescriptor nodeConstraint = ConstraintDescriptorFactory.existsForSchema(forLabel(1, 2), false)
                .withName("nodeConstraint");
        ConstraintDescriptor relConstraint = ConstraintDescriptorFactory.existsForSchema(forRelType(1, 2), false)
                .withName("relConstraint");
        String nodeExistenceConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> null, tokenRead, nodeConstraint);
        String relExistenceConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> null, tokenRead, relConstraint);

        assertThat(nodeExistenceConstraint)
                .isEqualTo("CREATE CONSTRAINT `nodeConstraint` FOR (n:`Label1`) REQUIRE (n.`property2`) IS NOT NULL");
        assertThat(relExistenceConstraint)
                .isEqualTo(
                        "CREATE CONSTRAINT `relConstraint` FOR ()-[r:`RelType1`]-() REQUIRE (r.`property2`) IS NOT NULL");
    }

    @Test
    void canGenerateSchemaStatementForTypeConstraints() throws KernelException {
        ConstraintDescriptor nodeConstraint = ConstraintDescriptorFactory.typeForSchema(
                        forLabel(1, 2), PropertyTypeSet.of(SchemaValueType.INTEGER), false)
                .withName("nodeConstraint");
        ConstraintDescriptor relConstraint = ConstraintDescriptorFactory.typeForSchema(
                        forRelType(1, 2), PropertyTypeSet.of(SchemaValueType.BOOLEAN), false)
                .withName("relConstraint");
        String nodePropertyTypeConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> null, tokenRead, nodeConstraint);
        String relPropertyTypeConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> null, tokenRead, relConstraint);

        assertThat(nodePropertyTypeConstraint)
                .isEqualTo("CREATE CONSTRAINT `nodeConstraint` FOR (n:`Label1`) REQUIRE (n.`property2`) IS :: INTEGER");
        assertThat(relPropertyTypeConstraint)
                .isEqualTo(
                        "CREATE CONSTRAINT `relConstraint` FOR ()-[r:`RelType1`]-() REQUIRE (r.`property2`) IS :: BOOLEAN");
    }

    @Test
    void canGenerateEscapedSchemaStatementForDefaultIndex() throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(
                        forLabel(ESC_BACKTICK_ID, ESC_BACKTICK_ID), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName(NAME_WITH_BACKTICKS)
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(
                        forRelType(ESC_BACKTICK_ID, ESC_BACKTICK_ID), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName(NAME_WITH_BACKTICKS)
                .materialise(2);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo("CREATE RANGE INDEX " + ESCAPED_NAME_WITH_BACKTICKS + " FOR (n:"
                        + ESCAPED_LABEL_WITH_BACKTICKS + ") ON (n." + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS
                        + ") OPTIONS {}");
        assertThat(typeIndex)
                .isEqualTo("CREATE RANGE INDEX " + ESCAPED_NAME_WITH_BACKTICKS + " FOR ()-[r:"
                        + ESCAPED_TYPE_WITH_BACKTICKS + "]-() ON (r." + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS
                        + ") OPTIONS {}");
    }

    @EnumSource(value = PropertyIndex.class)
    @ParameterizedTest
    void canGenerateEscapedSchemaStatementForPropertyIndex(PropertyIndex index) throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(
                        forLabel(ESC_BACKTICK_ID, ESC_BACKTICK_ID), index.descriptor)
                .withName(NAME_WITH_BACKTICKS)
                .withIndexType(index.indexType)
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(
                        forRelType(ESC_BACKTICK_ID, ESC_BACKTICK_ID), index.descriptor)
                .withName(NAME_WITH_BACKTICKS)
                .withIndexType(index.indexType)
                .materialise(2);

        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo("CREATE " + index.indexType + " INDEX " + ESCAPED_NAME_WITH_BACKTICKS + " FOR (n:"
                        + ESCAPED_LABEL_WITH_BACKTICKS + ") ON (n." + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS
                        + ") OPTIONS {}");
        assertThat(typeIndex)
                .isEqualTo("CREATE " + index.indexType + " INDEX " + ESCAPED_NAME_WITH_BACKTICKS + " FOR ()-[r:"
                        + ESCAPED_TYPE_WITH_BACKTICKS + "]-() ON (r." + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS
                        + ") OPTIONS {}");
    }

    @Test
    void canGenerateEscapedSchemaStatementForLookupIndex() throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(
                        ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR, AllIndexProviderDescriptors.TOKEN_DESCRIPTOR)
                .withIndexType(LOOKUP)
                .withName(NAME_WITH_BACKTICKS)
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(
                        ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR, AllIndexProviderDescriptors.TOKEN_DESCRIPTOR)
                .withIndexType(LOOKUP)
                .withName(NAME_WITH_BACKTICKS)
                .materialise(2);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo("CREATE LOOKUP INDEX " + ESCAPED_NAME_WITH_BACKTICKS + " FOR (n) ON EACH labels(n)");
        assertThat(typeIndex)
                .isEqualTo("CREATE LOOKUP INDEX " + ESCAPED_NAME_WITH_BACKTICKS + " FOR ()-[r]-() ON EACH type(r)");
    }

    @Test
    void canGenerateEscapedSchemaStatementForFulltextIndex() throws KernelException {
        IndexDescriptor nodeIndex = IndexPrototype.forSchema(
                        SchemaDescriptors.forSemanticSearch(
                                NODE, new int[] {ESC_BACKTICK_ID}, new int[] {ESC_BACKTICK_ID}),
                        AllIndexProviderDescriptors.FULLTEXT_V1_DESCRIPTOR)
                .withIndexType(FULLTEXT)
                .withName(NAME_WITH_BACKTICKS)
                .materialise(1);
        IndexDescriptor relIndex = IndexPrototype.forSchema(
                        SchemaDescriptors.forSemanticSearch(
                                RELATIONSHIP, new int[] {ESC_BACKTICK_ID}, new int[] {ESC_BACKTICK_ID}),
                        AllIndexProviderDescriptors.FULLTEXT_V2_DESCRIPTOR)
                .withIndexType(FULLTEXT)
                .withName(NAME_WITH_BACKTICKS)
                .materialise(2);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);
        String typeIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, relIndex);

        assertThat(labelIndex)
                .isEqualTo("CREATE FULLTEXT INDEX " + ESCAPED_NAME_WITH_BACKTICKS + " FOR (n:"
                        + ESCAPED_LABEL_WITH_BACKTICKS + ") " + "ON EACH [n." + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS
                        + "] OPTIONS {}");
        assertThat(typeIndex)
                .isEqualTo("CREATE FULLTEXT INDEX " + ESCAPED_NAME_WITH_BACKTICKS + " FOR ()-[r:"
                        + ESCAPED_TYPE_WITH_BACKTICKS + "]-() " + "ON EACH [r." + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS
                        + "] OPTIONS {}");
    }

    @Test
    void canGenerateEscapedSchemaStatementForVectorIndex() throws KernelException {
        KernelVersion kernelVersion = LatestVersions.LATEST_KERNEL_VERSION;
        VectorIndexVersion version = VectorIndexVersion.latestSupportedVersion(kernelVersion);
        IndexConfig configFromUser = IndexSettingUtil.defaultConfigForTest(VECTOR.toPublicApi());
        VectorIndexConfig vectorIndexConfig = version.indexSettingValidator(kernelVersion)
                .validateToTypedConfig(new IndexConfigAccessor(configFromUser));

        IndexPrototype prototype = IndexPrototype.forSchema(
                        SchemaDescriptors.forLabel(ESC_BACKTICK_ID, ESC_BACKTICK_ID), version.descriptor())
                .withIndexType(VECTOR)
                .withName(NAME_WITH_BACKTICKS)
                .withIndexConfig(configFromUser);
        // simulate VectorIndexProvider::validateIndexPrototype
        prototype = prototype.withIndexConfig(vectorIndexConfig.config());

        IndexDescriptor nodeIndex = prototype.materialise(1);
        String labelIndex = SchemaStatementCreator.getIndexSchemaStatement(tokenRead, nodeIndex);

        assertThat(labelIndex).isEqualToIgnoringWhitespace("""
                        CREATE VECTOR INDEX %s
                        FOR (n:%s) ON (n.%s)
                        OPTIONS {
                            indexConfig: {
                                `%s`: %s,
                                `%s`: %s,
                                `%s`: %s,
                                `%s`: %s,
                                `%s`: '%s',
                                `%s`: '%s'
                            }
                        }
                        """.formatted(
                        ESCAPED_NAME_WITH_BACKTICKS,
                        ESCAPED_LABEL_WITH_BACKTICKS,
                        ESCAPED_PROPERTY_KEY_WITH_BACKTICKS,
                        VECTOR_DEFAULT_SEARCH_EXPANSION_FACTOR.getSettingName(),
                        Double.toString(vectorIndexConfig.defaultSearchExpansionFactor()),
                        VECTOR_DIMENSIONS.getSettingName(),
                        Integer.toString(vectorIndexConfig.dimensions().orElseThrow()),
                        VECTOR_HNSW_EF_CONSTRUCTION.getSettingName(),
                        Integer.toString(vectorIndexConfig.hnsw().efConstruction()),
                        VECTOR_HNSW_M.getSettingName(),
                        Integer.toString(vectorIndexConfig.hnsw().M()),
                        VECTOR_QUANTIZATION_TYPE.getSettingName(),
                        vectorIndexConfig.quantization().name(),
                        VECTOR_SIMILARITY_FUNCTION.getSettingName(),
                        vectorIndexConfig.similarityFunction().functionName()));
    }

    @Test
    void canGenerateEscapedSchemaStatementForNodeKeyConstraint() throws KernelException {
        ConstraintDescriptor nodeConstraint = ConstraintDescriptorFactory.nodeKeyForLabel(
                        ESC_BACKTICK_ID, ESC_BACKTICK_ID)
                .withName(NAME_WITH_BACKTICKS);
        IndexDescriptor nodeIndex = IndexPrototype.uniqueForSchema(
                        forLabel(ESC_BACKTICK_ID, ESC_BACKTICK_ID), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("nodeIndex")
                .materialise(1);

        String nodeKeyConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> nodeIndex, tokenRead, nodeConstraint);

        assertThat(nodeKeyConstraint)
                .isEqualTo("CREATE CONSTRAINT " + ESCAPED_NAME_WITH_BACKTICKS + " FOR (n:"
                        + ESCAPED_LABEL_WITH_BACKTICKS + ") " + "REQUIRE (n."
                        + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS + ") "
                        + "IS NODE KEY OPTIONS {}");
    }

    @Test
    void canGenerateEscapedSchemaStatementForRelKeyConstraint() throws KernelException {
        ConstraintDescriptor relConstraint = ConstraintDescriptorFactory.keyForSchema(
                        SchemaDescriptors.forRelType(ESC_BACKTICK_ID, ESC_BACKTICK_ID))
                .withName(NAME_WITH_BACKTICKS);
        IndexDescriptor relIndex = IndexPrototype.uniqueForSchema(
                        forRelType(ESC_BACKTICK_ID, ESC_BACKTICK_ID), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("relIndex")
                .materialise(1);

        String relKeyConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> relIndex, tokenRead, relConstraint);

        assertThat(relKeyConstraint)
                .isEqualTo("CREATE CONSTRAINT " + ESCAPED_NAME_WITH_BACKTICKS + " FOR ()-[r:"
                        + ESCAPED_TYPE_WITH_BACKTICKS + "]-() " + "REQUIRE (r."
                        + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS + ") "
                        + "IS RELATIONSHIP KEY OPTIONS {}");
    }

    @Test
    void canGenerateEscapedSchemaStatementForNodeUniquenessConstraint() throws KernelException {
        ConstraintDescriptor nodeConstraint = ConstraintDescriptorFactory.uniqueForLabel(
                        ESC_BACKTICK_ID, ESC_BACKTICK_ID)
                .withName(NAME_WITH_BACKTICKS);
        IndexDescriptor nodeIndex = IndexPrototype.uniqueForSchema(
                        forLabel(ESC_BACKTICK_ID, ESC_BACKTICK_ID), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("nodeIndex")
                .materialise(1);

        String uniqueConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> nodeIndex, tokenRead, nodeConstraint);

        assertThat(uniqueConstraint)
                .isEqualTo("CREATE CONSTRAINT " + ESCAPED_NAME_WITH_BACKTICKS + " FOR (n:"
                        + ESCAPED_LABEL_WITH_BACKTICKS + ") " + "REQUIRE (n." + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS
                        + ") IS UNIQUE OPTIONS {}");
    }

    @Test
    void canGenerateEscapedSchemaStatementForRelUniquenessConstraint() throws KernelException {
        ConstraintDescriptor relConstraint = ConstraintDescriptorFactory.uniqueForSchema(
                        SchemaDescriptors.forRelType(ESC_BACKTICK_ID, ESC_BACKTICK_ID))
                .withName(NAME_WITH_BACKTICKS);
        IndexDescriptor relIndex = IndexPrototype.uniqueForSchema(
                        forRelType(ESC_BACKTICK_ID, ESC_BACKTICK_ID), AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
                .withName("relIndex")
                .materialise(1);

        String uniqueConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> relIndex, tokenRead, relConstraint);

        assertThat(uniqueConstraint)
                .isEqualTo("CREATE CONSTRAINT " + ESCAPED_NAME_WITH_BACKTICKS + " FOR ()-[r:"
                        + ESCAPED_TYPE_WITH_BACKTICKS + "]-() " + "REQUIRE (r." + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS
                        + ") IS UNIQUE OPTIONS {}");
    }

    @Test
    void canGenerateEscapedSchemaStatementForExistenceConstraints() throws KernelException {
        ConstraintDescriptor nodeConstraint = ConstraintDescriptorFactory.existsForSchema(
                        forLabel(ESC_BACKTICK_ID, ESC_BACKTICK_ID), false)
                .withName(NAME_WITH_BACKTICKS);
        ConstraintDescriptor relConstraint = ConstraintDescriptorFactory.existsForSchema(
                        forRelType(ESC_BACKTICK_ID, ESC_BACKTICK_ID), false)
                .withName(NAME_WITH_BACKTICKS);
        String nodeExistenceConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> null, tokenRead, nodeConstraint);
        String relExistenceConstraint =
                SchemaStatementCreator.getConstraintSchemaStatement(name -> null, tokenRead, relConstraint);

        assertThat(nodeExistenceConstraint)
                .isEqualTo(
                        "CREATE CONSTRAINT " + ESCAPED_NAME_WITH_BACKTICKS + " FOR (n:" + ESCAPED_LABEL_WITH_BACKTICKS
                                + ") " + "REQUIRE (n." + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS + ") IS NOT NULL");
        assertThat(relExistenceConstraint)
                .isEqualTo("CREATE CONSTRAINT " + ESCAPED_NAME_WITH_BACKTICKS + " FOR " + "()-[r:"
                        + ESCAPED_TYPE_WITH_BACKTICKS + "]-() REQUIRE " + "(r."
                        + ESCAPED_PROPERTY_KEY_WITH_BACKTICKS + ") IS NOT NULL");
    }

    private static IndexDescriptor addIndexConfig(IndexDescriptor index) {
        IndexConfig indexConfig = index.getIndexConfig();

        Map<String, Value> spatialConfig = new HashMap<>();
        ConfiguredSpaceFillingCurveSettingsCache settingsCache =
                new ConfiguredSpaceFillingCurveSettingsCache(Config.defaults());
        SpaceFillingCurveSettings spaceFillingCurveSettings = settingsCache.forCRS(CoordinateReferenceSystem.CARTESIAN);
        SpatialIndexConfig.addSpatialConfig(
                spatialConfig,
                CoordinateReferenceSystem.CARTESIAN,
                spaceFillingCurveSettings.indexExtents().min(),
                spaceFillingCurveSettings.indexExtents().max());
        for (Entry<String, Value> entry : spatialConfig.entrySet()) {
            indexConfig = indexConfig.withIfAbsent(entry.getKey(), entry.getValue());
        }
        return index.withIndexConfig(indexConfig);
    }

    private enum PropertyIndex {
        RANGE(IndexType.RANGE),
        TEXT(IndexType.TEXT),
        POINT(IndexType.POINT);

        private final IndexType indexType;
        private final IndexProviderDescriptor descriptor;

        PropertyIndex(IndexType indexType) {
            this.indexType = indexType;
            this.descriptor = AllIndexProviderDescriptors.LATEST_INDEX_PROVIDERS.get(indexType);
        }
    }
}
