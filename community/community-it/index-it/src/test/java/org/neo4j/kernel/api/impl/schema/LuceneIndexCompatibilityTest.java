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
package org.neo4j.kernel.api.impl.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.InstanceOfAssertFactories.BOOLEAN;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.always_use_latest_index_provider;
import static org.neo4j.configuration.GraphDatabaseSettings.default_language;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.internal.helpers.NameUtil.escapeSingleQuotes;
import static org.neo4j.internal.helpers.NameUtil.forceEscapeName;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.FULLTEXT_V1_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.FULLTEXT_V2_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.INDEX_TYPES;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.TEXT_V2_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.TEXT_V3_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.VECTOR_V1_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.VECTOR_V2_DESCRIPTOR;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.VECTOR_V3_DESCRIPTOR;
import static org.neo4j.kernel.impl.util.ValueUtils.wrapNodeEntity;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.random.RandomGenerator;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.text.StringSubstitutor;
import org.assertj.core.api.Assert;
import org.assertj.core.api.IteratorAssert;
import org.assertj.core.api.MapAssert;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings.CypherVersion;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexRef;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.RequireAlignedFormat;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.extension.SkipOnSpd.Note;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.values.storable.RandomValuesUtils;
import org.neo4j.values.utils.PrettyPrinter;

/**
 * Tests that existing, supported, older Lucene indexes can still be read and written to.
 * <p>
 * This test uses an evolving Neo4j dump, that emulates a supported migration path for a user, adding new valid Lucene
 * index tuples in the versions they are added. As new tuples are needed, this test can be used to update the dump
 * accordingly, though manual moving to the resource path is required.
 * <p>
 * To keep things simple, each index is handling its own subgraph of nodes, with no overlap. This is done by the node
 * label used for the schema and relavant data, is the same as the index name. The index name is generated based on
 * the provided index description. For example:
 * <pre><code>
 * Index.describe(
 *     VECTOR_V2_DESCRIPTOR,
 *     Neo4jVersion.V5_23,
 *     Additions.SQ, Additions.VectorCodecV2)
 * </code></pre>
 * <dl>
 *   <dt>Index name:</dt>
 *   <dd><code>`5.23, vector-2.0, Lucene 9, SQ, VectorCodecV2`</code></dd>
 *   <dt>Index provider:</dt>
 *   <dd><code>"vector-2.0"</code></dd>
 *   <dt>Index config:</dt>
 *   <dd><code>`vector.quantization.enabled`: true</code></dd>
 *   <dt>Internally configured:</dt>
 *   <dd><dl>
 *       <dt>Codec used:</dt>
 *       <dd><code>VectorCodecV2</code></dd>
 *   </dl></dd>
 *   <dt>Nodes have label:</dt>
 *   <dd><code>`5.23, vector-2.0, Lucene 9, SQ, VectorCodecV2`</code></dd>
 *   <dt>Nodes have properties:</dt>
 *   <dd><ul>
 *     <li><code>`text`: 'Written in 5.23'</code></li>
 *     <li><code>`vector`: [1.f, \d, rand()]</code></li>
 *   </ul></dd>
 * </dl>
 */
@RequireAlignedFormat
@SkipOnSpd(
        reason = "Test uses existing ALIGNED dump to check existing old indexes still work",
        notes = Note.incompatible)
@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class LuceneIndexCompatibilityTest {
    private static final String DUMP = "lucene-index-compatibility.dump";
    private static final Collection<Index> INDEXES = List.of(
            // TEXT
            Index.describe(TEXT_V1_DESCRIPTOR, Neo4jVersion.V4_4),
            Index.describe(TEXT_V1_DESCRIPTOR, Neo4jVersion.V5_1),
            Index.describe(TEXT_V2_DESCRIPTOR, Neo4jVersion.V5_1),
            Index.describe(TEXT_V3_DESCRIPTOR, Neo4jVersion.V2025_09),
            // FULLTEXT
            Index.describe(FULLTEXT_V1_DESCRIPTOR, Neo4jVersion.V4_0),
            Index.describe(FULLTEXT_V1_DESCRIPTOR, Neo4jVersion.V5_1),
            Index.describe(FULLTEXT_V2_DESCRIPTOR, Neo4jVersion.V2025_09),
            // VECTOR
            Index.describe(VECTOR_V1_DESCRIPTOR, Neo4jVersion.V5_11),
            Index.describe(VECTOR_V1_DESCRIPTOR, Neo4jVersion.V5_14, Addition.VectorCodecV1),
            Index.describe(VECTOR_V2_DESCRIPTOR, Neo4jVersion.V5_18, Addition.VectorCodecV1),
            Index.describe(VECTOR_V2_DESCRIPTOR, Neo4jVersion.V5_23, Addition.VectorCodecV2),
            Index.describe(VECTOR_V2_DESCRIPTOR, Neo4jVersion.V5_23, Addition.SQ, Addition.VectorCodecV2),
            Index.describe(VECTOR_V3_DESCRIPTOR, Neo4jVersion.V2025_09, Addition.VectorCodecV3),
            Index.describe(VECTOR_V3_DESCRIPTOR, Neo4jVersion.V2025_09, Addition.SQ, Addition.VectorCodecV3));

    private static final int EXISTING_NODES_PER_INDEX = 5;
    private static final String TEXT_PREFIX = "Written in ";
    private static final Pattern EXISTING_TEXT = Pattern.compile(TEXT_PREFIX + "\\d+(\\.\\d+)*");
    private static final String NEW_TEXT = TEXT_PREFIX + Neo4jVersion.LATEST;
    private static final float[] VECTOR = new float[] {1.0f, 0.0f, 0.0f};
    private static final Label LAST_TOUCHED = Label.label("LastTouched");
    private static final int MAX_EF_SEARCH = 1 << 20;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Inject
    RandomSupport random;

    private Config config;
    private DatabaseLayout layout;
    private DatabaseManagementService dbms;
    private GraphDatabaseService db;

    @BeforeEach
    void setUp() throws Exception {
        config = Config.newBuilder()
                .set(neo4j_home, directory.homePath())
                .set(default_language, CypherVersion.Cypher25)
                .set(always_use_latest_index_provider, false)
                .build();
        layout = DatabaseLayout.of(config);
        fs.mkdirs(layout.databaseDirectory());
        fs.mkdirs(layout.getNeo4jLayout().transactionLogsRootDirectory());

        Path dump =
                Path.of(getClass().getResource(DUMP).toURI()).toAbsolutePath().normalize();
        Loader loader = new Loader(fs);
        loader.load(layout, dump);
        startDBMS();

        random.withConfiguration(RandomValuesUtils.selectStorageEngineDependentConfiguration(db))
                .reset();
    }

    void startDBMS() {
        dbms = new TestDatabaseManagementServiceBuilder(layout)
                .setConfig(config)
                .build();
        db = dbms.database(layout.getDatabaseName());
    }

    @AfterEach
    void stopDBMS() {
        db = null;
        if (dbms != null) {
            dbms.shutdown();
        }
    }

    private static Stream<Index> indexes() {
        return INDEXES.stream();
    }

    @ParameterizedTest
    @MethodSource("indexes")
    @Disabled("Helper method for debugging, not intended to be run as regular test")
    void printKnownIndexes(Index index) {
        System.out.printf("%s%n%n", index);
    }

    @Test
    @Disabled("Helper method for debugging, not intended to be run as regular test")
    void printAllIndexesInDump() {
        try (Transaction tx = db.beginTx()) {
            SortedSet<IndexDefinition> indexes = existingLuceneNodeIndexes(tx);
            for (IndexDefinition index : indexes) {
                System.out.printf("%s%n%n", index);
            }
        }
    }

    @Test
    @Disabled("Helper method for debugging, not intended to be run as regular test")
    void printAllNodesInDump() {
        PrettyPrinter pp = new PrettyPrinter();
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> nodes = tx.getAllNodes()) {
            for (Node node : nodes) {
                pp.reset();
                wrapNodeEntity(node).writeTo(pp);
                System.out.println(pp.value());
            }
        }
    }

    @Test
    void ensureConfiguredIndexesExistInDump() {
        Map<String, Index> configuredIndexes = new HashMap<>();
        for (Index index : INDEXES) {
            assertThat(configuredIndexes.put(index.name(), index))
                    .as("index with name `%s` should only be seen once", index.name())
                    .isNull();
        }

        try (Transaction tx = db.beginTx()) {
            Map<String, IndexDefinition> indexesInDump = new HashMap<>();
            for (IndexDefinition index : existingLuceneNodeIndexes(tx)) {
                assertThat(indexesInDump.put(index.getName(), index))
                        .as("index with name `%s` should only be seen once", index.getName())
                        .isNull();
            }

            Set<String> names = configuredIndexes.keySet();
            assertThat(indexesInDump).containsOnlyKeys(names);

            for (String name : names) {
                Index configuredIndex = assertThat(configuredIndexes)
                        .extractingByKey(name)
                        .isNotNull()
                        .actual();
                IndexDefinition indexInDump = assertThat(indexesInDump)
                        .extractingByKey(name)
                        .isNotNull()
                        .actual();
                ObjectAssert<IndexDefinition> indexAssert = assertThat(indexInDump);

                indexAssert.extracting(IndexDefinition::getName).isEqualTo(configuredIndex.name());
                indexAssert.extracting(IndexDefinition::isNodeIndex, BOOLEAN).isTrue();
                indexAssert
                        .extracting(IndexDefinition::getIndexType)
                        .extracting(IndexType::fromPublicApi)
                        .isSameAs(configuredIndex.type());
                indexAssert
                        .extracting(IndexDefinition::getLabels, list(Label.class))
                        .singleElement()
                        .isEqualTo(configuredIndex.label());
                indexAssert
                        .extracting(IndexDefinition::getPropertyKeys, list(String.class))
                        .singleElement()
                        .isEqualTo(configuredIndex.key().name());

                indexAssert
                        .asInstanceOf(type(IndexDefinitionImpl.class))
                        .extracting(IndexDefinitionImpl::getIndexReference)
                        .extracting(IndexRef::getIndexProvider)
                        .isEqualTo(configuredIndex.provider());

                SortedSet<Addition> additions = configuredIndex.additions();
                MapAssert<IndexSetting, Object> indexConfigAssert = indexAssert.extracting(
                        IndexDefinition::getIndexConfiguration, map(IndexSetting.class, Object.class));

                if (configuredIndex.type() == IndexType.VECTOR) {
                    for (Addition addition : additions) {
                        Assert<?, ?> ignored =
                                switch (addition) {
                                    case SQ ->
                                        indexConfigAssert
                                                .extractingByKey(IndexSetting.vector_Quantization_Enabled(), BOOLEAN)
                                                .isTrue();

                                    case VectorCodecV1, VectorCodecV2, VectorCodecV3 ->
                                        null; // cannot easily check the codec
                                };
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("indexes")
    void shouldReadAndWriteToIndex(Index index) {
        Map<Long, Object> existing;
        try (Transaction tx = db.beginTx()) {
            existing = assertExistingText(tx, index);
        }

        int numberOfNewNodes = random.nextInt(4, 8);
        try (Transaction tx = db.beginTx()) {
            index.createNewNodes(tx, numberOfNewNodes, random.random());
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            assertNewText(tx, index, existing.keySet(), numberOfNewNodes);
        }
    }

    /// Modify the `INDEXES` array with the new index definitions and _manually_ run this test.
    /// It will create the dump in the working directory of the test, _overwriting it_ if it exists.
    /// Modifying the `dump` varible will change the output location.
    /// It will require manual moving to the resource directory for this test to overwrite the existing dump.
    @Test
    @Disabled("Helper method for creating an updated dump, not intended to be run as regular test")
    void createUpdatedDump() throws IOException, KernelException {
        Path dump = Path.of(System.getProperty("user.dir"), DUMP);

        Map<String, Index> configuredIndexes = new HashMap<>();
        for (Index index : INDEXES) {
            assertThat(configuredIndexes.put(index.name(), index))
                    .as("index with name `%s` should only be seen once", index.name())
                    .isNull();
        }

        try (Transaction tx = db.beginTx()) {
            for (IndexDefinition index : existingLuceneNodeIndexes(tx)) {
                assertThat(configuredIndexes.remove(index.getName()))
                        .as("index with name `%s` should have been configured")
                        .isNotNull();
            }
        }

        Collection<Index> indexesToCreate = configuredIndexes.values();
        if (indexesToCreate.isEmpty()) {
            return;
        }

        System.out.println("Creating nodes for indexes");
        try (Transaction tx = db.beginTx()) {
            for (Index index : indexesToCreate) {
                index.createNodes(tx, random.random());
            }
            tx.commit();
        }

        System.out.printf("Creating %d indexes:%n", indexesToCreate.size());
        try (Transaction tx = db.beginTx()) {
            for (Index index : indexesToCreate) {
                System.out.println(index);
                index.createIndex(tx);
            }
            tx.commit();
        }

        stopDBMS();

        System.out.printf("%s updated dump: %s", fs.fileExists(dump) ? "Overwritting with" : "Creating", dump);
        Dumper dumper = new Dumper(fs);
        Path[] exclude = new Path[] {
            layout.databaseLockFile().getFileName(), layout.quarantineFile().getFileName()
        };
        dumper.dump(
                Dumper.FileOutput.of(fs, dump),
                DumpFormatSelector.selectWriteFormat(),
                Dumper.collectManifest(
                        layout.databaseDirectory(),
                        layout.getTransactionLogsDirectory(),
                        path -> ArrayUtil.contains(exclude, path.getFileName())));
    }

    private static SortedSet<IndexDefinition> existingLuceneNodeIndexes(Transaction tx) {
        Set<IndexType> luceneIndexTypes = Set.of(IndexType.TEXT, IndexType.FULLTEXT, IndexType.VECTOR);
        SortedSet<IndexDefinition> indexes = new TreeSet<>(
                Comparator.comparing(IndexDefinition::getIndexType).thenComparing(IndexDefinition::getName));
        for (IndexDefinition index : tx.schema().getIndexes()) {
            IndexType type = IndexType.fromPublicApi(index.getIndexType());
            if (index.isNodeIndex() && luceneIndexTypes.contains(type)) {
                indexes.add(index);
            }
        }
        return indexes;
    }

    private static Map<Long, Object> assertExistingText(Transaction tx, Index index) {
        Map<Long, Object> allEntries = allEntries(tx, index, IndexPropKey.text);
        Map<Long, Object> cypherIndexQuery = query(tx, index, IndexPropKey.text);
        assertExistingText(allEntries).hasSize(EXISTING_NODES_PER_INDEX);
        assertExistingText(cypherIndexQuery).hasSameSizeAs(allEntries);
        return allEntries;
    }

    private static MapAssert<Long, ?> assertExistingText(Map<Long, ?> projections) {
        return assertThat(projections)
                .allSatisfy((nodeId, projection) ->
                        assertThat(projection).asInstanceOf(STRING).matches(EXISTING_TEXT));
    }

    private static void assertNewText(Transaction tx, Index index, Set<Long> existing, int expectedNew) {
        Map<Long, Object> newEntries = allEntries(tx, index, IndexPropKey.text);
        Map<Long, Object> cypherIndexQuery = query(tx, index, IndexPropKey.text);
        existing.forEach(nodeId -> {
            assertThat(newEntries.remove(nodeId))
                    .as("Existing entry for %d should have been seen", nodeId)
                    .isNotNull();
            assertThat(cypherIndexQuery.remove(nodeId))
                    .as("Existing entry for %d should have been seen", nodeId)
                    .isNotNull();
        });
        assertNewText(newEntries).hasSize(expectedNew);
        assertNewText(cypherIndexQuery).hasSameSizeAs(newEntries);
    }

    private static MapAssert<Long, ?> assertNewText(Map<Long, ?> projections) {
        return assertThat(projections)
                .allSatisfy((nodeId, projection) ->
                        assertThat(projection).asInstanceOf(STRING).matches(NEW_TEXT));
    }

    private static Map<Long, Object> allEntries(Transaction tx, Index index, IndexPropKey projectionKey) {
        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        IndexDescriptor indexDescriptor = ktx.schemaRead().indexGetForName(index.name());
        assertThat(indexDescriptor).as("Index `%s` should exist", index.name()).isNotEqualTo(IndexDescriptor.NO_INDEX);
        int labelId = ktx.tokenRead().nodeLabel(index.label().name());
        assertThat(labelId).as("Label `%s` should exist", index.label().name()).isNotEqualTo(TokenConstants.NO_TOKEN);
        int propKeyId = ktx.tokenRead().propertyKey(index.key().name());
        assertThat(propKeyId)
                .as("Property Key `%s` should exist", index.key().name())
                .isNotEqualTo(TokenConstants.NO_TOKEN);

        int projectionKeyId;
        if (projectionKey == index.key()) {
            projectionKeyId = propKeyId;
        } else {
            projectionKeyId = ktx.tokenRead().propertyKey(projectionKey.name());
            assertThat(projectionKeyId)
                    .as("Property Key `%s` should exist", projectionKey.name())
                    .isNotEqualTo(TokenConstants.NO_TOKEN);
        }
        PropertySelection selection = PropertySelection.selection(projectionKeyId);

        Map<Long, Object> projections = new HashMap<>();
        assertThatCode(() -> {
                    try (NodeValueIndexCursor indexCursor = ktx.cursors()
                                    .allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker());
                            NodeCursor nodeCursor =
                                    ktx.cursors().allocateNodeCursor(ktx.cursorContext(), ktx.memoryTracker());
                            PropertyCursor propertyCursor =
                                    ktx.cursors().allocatePropertyCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                        ktx.dataRead()
                                .nodeIndexSeek(
                                        ktx.queryContext(),
                                        ktx.dataRead().indexReadSession(indexDescriptor),
                                        indexCursor,
                                        IndexQueryConstraints.unconstrained(),
                                        PropertyIndexQuery.allEntries());
                        while (indexCursor.next()) {
                            long nodeId = indexCursor.nodeReference();
                            indexCursor.node(nodeCursor);
                            assertThat(nodeCursor.next())
                                    .as("node %s should exist", nodeId)
                                    .isTrue();
                            nodeCursor.properties(propertyCursor, selection);
                            assertThat(propertyCursor.next())
                                    .as("properties matching %s should exist on node %d", selection, nodeId)
                                    .isTrue();
                            assertThat(projections.put(
                                            nodeId,
                                            propertyCursor.propertyValue().asObjectCopy()))
                                    .as("node %d should only be seen once", nodeId)
                                    .isNull();
                        }
                    }
                })
                .doesNotThrowAnyException();
        return projections;
    }

    private static Map<Long, Object> query(Transaction tx, Index index, IndexPropKey projectionKey) {
        return switch (index.type()) {
            case TEXT -> textQuery(tx, index, projectionKey);
            case FULLTEXT -> fulltextQuery(tx, index, projectionKey);
            case VECTOR -> vectorQuery(tx, index, projectionKey);
            default -> throw new IllegalStateException("Unexpected index type: " + index.type());
        };
    }

    private static Map<Long, Object> textQuery(Transaction tx, Index index, IndexPropKey projectionKey) {
        // Cannot use cypher query parameters with USING INDEX hint
        // Cannot mix $($label) pattern with literals in query
        String query = """
            PROFILE
            MATCH (node:${label})
            USING TEXT INDEX node:${label}(${key})
            WHERE node.${key} STARTS WITH ${prefix}
            RETURN id(node) AS nodeId, node.${projectionKey} AS projection
            """;
        Map<String, Object> params = Map.of(
                "label", forceEscapeName(index.label().name()),
                "key", forceEscapeName(index.key().name()),
                "prefix", "'" + escapeSingleQuotes(TEXT_PREFIX) + "'",
                "projectionKey", forceEscapeName(projectionKey.name()));
        return query(tx, StringSubstitutor.replace(query, params), Map.of());
    }

    private static Map<Long, Object> fulltextQuery(Transaction tx, Index index, IndexPropKey projectionKey) {
        String query = """
            CALL db.index.fulltext.queryNodes($name, $prefix) YIELD node, score
            RETURN id(node) AS nodeId, node[$projectionKey] AS projection
            """;
        Map<String, Object> params =
                Map.of("name", index.name(), "prefix", TEXT_PREFIX + "*", "projectionKey", projectionKey.name());
        return query(tx, query, params);
    }

    private static Map<Long, Object> vectorQuery(Transaction tx, Index index, IndexPropKey projectionKey) {
        // index name cannot be a parameter here yet
        String query = """
            MATCH (node:$($label))
            SEARCH node IN (
                VECTOR INDEX ${name}
                FOR $vector
                LIMIT $efSearch
            ) SCORE as score
            RETURN id(node) AS nodeId, node[$projectionKey] AS projection
            """;
        Map<String, Object> name = Map.of("name", forceEscapeName(index.name()));
        Map<String, Object> params = Map.of(
                "label",
                index.label().name(),
                "vector",
                VECTOR,
                "efSearch",
                MAX_EF_SEARCH,
                "projectionKey",
                projectionKey.name());
        return query(tx, StringSubstitutor.replace(query, name), params);
    }

    private static Map<Long, Object> query(Transaction tx, String query, Map<String, Object> params) {
        Map<Long, Object> projections = new HashMap<>();
        try (Result result = tx.execute(query, params)) {
            assertThat(result.getQueryExecutionType().canContainResults()).isTrue();
            result.forEachRemaining(row -> {
                MapAssert<String, Object> rowAssert = assertThat(row);
                long nodeId = rowAssert
                        .as("has nodeId")
                        .extractingByKey("nodeId", LONG)
                        .isNotNull()
                        .actual();
                Object projection = rowAssert
                        .as("has projection")
                        .extractingByKey("projection")
                        .isNotNull()
                        .actual();
                assertThat(projections.put(nodeId, projection))
                        .as("node %d should only be seen once", nodeId)
                        .isNull();
            });
            if (result.getQueryExecutionType().requestedExecutionPlanDescription()) {
                ExecutionPlanDescription plan = result.getExecutionPlanDescription();
                assertThat(planContains(plan, "NodeIndexSeekByRange"))
                        .as("Plan uses index")
                        .isTrue();
            }
        }
        return projections;
    }

    private static boolean planContains(ExecutionPlanDescription plan, String name) {
        for (ExecutionPlanDescription child : plan.getChildren()) {
            if (name.equals(child.getName()) || planContains(child, name)) {
                return true;
            }
        }
        return false;
    }

    private static void touch(Transaction tx, Neo4jVersion version) {
        try (ResourceIterator<Node> nodes = tx.findNodes(LAST_TOUCHED)) {
            IteratorAssert<Node> nodesAssert = assertThat(nodes);
            nodesAssert.as("Node with label :%s should exist", LAST_TOUCHED).hasNext();
            Node lastTouched = nodes.next();
            nodesAssert
                    .as("Only one node with label :%s should exist", LAST_TOUCHED)
                    .isExhausted();
            lastTouched.setProperty("version", version.toString());
        }
    }

    private enum Neo4jVersion {
        V4_0(LuceneVersion.V8),
        V4_4(LuceneVersion.V8),
        V5_1(LuceneVersion.V9),
        V5_11(LuceneVersion.V9),
        V5_14(LuceneVersion.V9),
        V5_18(LuceneVersion.V9),
        V5_23(LuceneVersion.V9),
        V2025_09(LuceneVersion.V10),

        LATEST(Arrays.asList(LuceneVersion.values()).getLast());

        private final LuceneVersion defaultLuceneVersion;

        Neo4jVersion(LuceneVersion defaultLuceneVersion) {
            this.defaultLuceneVersion = defaultLuceneVersion;
        }

        LuceneVersion defaultLuceneVersion() {
            return defaultLuceneVersion;
        }

        @Override
        public String toString() {
            return name().replace('_', '.');
        }
    }

    private enum LuceneVersion {
        V8,
        V9,
        V10;

        @Override
        public String toString() {
            return "Lucene " + name().substring(1);
        }
    }

    private enum IndexPropKey {
        text,
        vector;

        static IndexPropKey from(IndexType type) {
            return switch (type) {
                case TEXT, FULLTEXT -> IndexPropKey.text;
                case VECTOR -> IndexPropKey.vector;
                default -> throw new IllegalArgumentException("Unknown IndexPropKey for " + type + " index");
            };
        }
    }

    // please keep relative order static for this enum
    private enum Addition {
        // quantization
        SQ {
            @Override
            Map<IndexSetting, Object> settings() {
                return Map.of(IndexSetting.vector_Quantization_Enabled(), true);
            }
        },

        // codecs
        VectorCodecV1,
        VectorCodecV2,
        VectorCodecV3;

        Map<IndexSetting, Object> settings() {
            return Map.of();
        }
    }

    private record Index(
            Neo4jVersion version,
            IndexType type,
            IndexProviderDescriptor provider,
            LuceneVersion luceneVersion,
            String name,
            Label label,
            IndexPropKey key,
            SortedSet<Addition> additions) {

        static Index describe(IndexProviderDescriptor provider, Neo4jVersion version, Addition... additions) {
            IndexType type = INDEX_TYPES.get(provider);
            LuceneVersion luceneVersion = version.defaultLuceneVersion();
            IndexPropKey key = IndexPropKey.from(type);
            SortedSet<Addition> sortedAdditions =
                    additions != null ? new TreeSet<>(Arrays.asList(additions)) : new TreeSet<>();
            StringJoiner joiner = new StringJoiner(", ")
                    .add(version.toString())
                    .add(provider.name())
                    .add(luceneVersion.toString());
            sortedAdditions.forEach(addition -> joiner.add(addition.name()));
            String name = joiner.toString();
            Label label = Label.label(name);
            return new Index(version, type, provider, luceneVersion, name, label, key, sortedAdditions);
        }

        private void createNewNodes(Transaction tx, int numberOfNewNodesToCreate, RandomGenerator random) {
            createNodes(tx, Neo4jVersion.LATEST, numberOfNewNodesToCreate, random);
        }

        void createNodes(Transaction tx, RandomGenerator random) {
            createNodes(tx, version, EXISTING_NODES_PER_INDEX, random);
        }

        private void createNodes(
                Transaction tx, Neo4jVersion version, int numberOfNodesToCreate, RandomGenerator random) {
            for (int i = 1; i <= numberOfNodesToCreate; i++) {
                Node node = tx.createNode(label);
                node.setProperty(IndexPropKey.text.name(), TEXT_PREFIX + version);
                if (key == IndexPropKey.vector) {
                    node.setProperty(key.name(), new float[] {1.0f, i, random.nextFloat()});
                }
            }
            touch(tx, version);
        }

        void createIndex(Transaction tx) throws KernelException {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

            TokenWrite tokenWrite = ktx.tokenWrite();
            int labelId = tokenWrite.labelGetOrCreateForName(label.name());
            int propKeyId = tokenWrite.propertyKeyGetOrCreateForName(key.name());
            SchemaDescriptor schema =
                    switch (type) {
                        case TEXT -> SchemaDescriptors.forLabel(labelId, propKeyId);
                        case FULLTEXT, VECTOR ->
                            SchemaDescriptors.forSemanticSearch(
                                    EntityType.NODE, new int[] {labelId}, new int[] {propKeyId});
                        default -> throw new IllegalArgumentException(type + " is not a known Lucene index type");
                    };

            Map<IndexSetting, Object> settings = new HashMap<>();
            for (Addition addition : additions) {
                settings.putAll(addition.settings());
            }
            IndexConfig indexConfig = IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap(settings);

            IndexPrototype prototype = IndexPrototype.forSchema(schema)
                    .withName(name)
                    .withIndexType(type)
                    .withIndexProvider(provider)
                    .withIndexConfig(indexConfig);
            ktx.schemaWrite().indexCreate(prototype);
        }
    }
}
