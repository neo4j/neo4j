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
package org.neo4j.consistency.checking.full;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.test.Tokens.Factories.LABEL;
import static org.neo4j.test.Tokens.Factories.PROPERTY_KEY;
import static org.neo4j.test.Tokens.Factories.RELATIONSHIP_TYPE;
import static org.neo4j.test.mockito.mock.Property.property;
import static org.neo4j.test.mockito.mock.Property.set;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class SpecialisedIndexFullCheckTest {

    private static final Label INDEXED_LABEL = label("Label1");
    private static final RelationshipType INDEXED_TYPE = withName("Type1");
    private static final String PROP1 = "key1";
    private static final String PROP2 = "key2";

    @TestDirectoryExtension
    abstract static class TestBase {

        @Inject
        private TestDirectory testDirectory;

        protected GraphStoreFixture fixture;
        private final ByteArrayOutputStream logStream = new ByteArrayOutputStream();
        private final Log4jLogProvider logProvider = new Log4jLogProvider(logStream);

        protected final List<Long> indexedNodes = new ArrayList<>();
        private final List<Long> indexedRelationships = new ArrayList<>();
        protected final Map<Setting<?>, Object> settings = new HashMap<>();

        abstract IndexType type();

        abstract Object indexedValue();

        abstract Object anotherIndexedValue();

        abstract Object notIndexedValue();

        SchemaDescriptor nodeIndexSchema(int labelId, int propKeyId) {
            return SchemaDescriptors.forLabel(labelId, propKeyId);
        }

        IndexPrototype nodeIndex(KernelTransaction ktx, String propKey) throws KernelException {
            final var labelId = LABEL.getId(ktx, INDEXED_LABEL);
            final var propKeyId = PROPERTY_KEY.getId(ktx, propKey);
            return IndexPrototype.forSchema(nodeIndexSchema(labelId, propKeyId)).withIndexType(type());
        }

        void createNodeIndex(Transaction tx, String propertyKey) {
            final var ktx = ((InternalTransaction) tx).kernelTransaction();
            try {
                final var prototype = nodeIndex(ktx, propertyKey);
                if (prototype != null) {
                    ktx.schemaWrite().indexCreate(prototype);
                }
            } catch (KernelException e) {
                throw new RuntimeException(e);
            }
        }

        SchemaDescriptor relIndexSchema(int relTypeId, int propKeyId) {
            return SchemaDescriptors.forRelType(relTypeId, propKeyId);
        }

        IndexPrototype relIndex(KernelTransaction ktx, String propKey) throws KernelException {
            final var relTypeId = RELATIONSHIP_TYPE.getId(ktx, INDEXED_TYPE);
            final var propKeyId = PROPERTY_KEY.getId(ktx, propKey);
            return IndexPrototype.forSchema(relIndexSchema(relTypeId, propKeyId))
                    .withIndexType(type());
        }

        void createRelIndex(Transaction tx, String propertyKey) {
            final var ktx = ((InternalTransaction) tx).kernelTransaction();
            try {
                final var prototype = relIndex(ktx, propertyKey);
                if (prototype != null) {
                    ktx.schemaWrite().indexCreate(prototype);
                }
            } catch (KernelException e) {
                throw new RuntimeException(e);
            }
        }

        @BeforeEach
        protected void setUp() {
            fixture = createFixture();
        }

        @AfterEach
        void tearDown() {
            fixture.close();
        }

        @Test
        void shouldCheckConsistencyOfAConsistentStore() throws Exception {
            ConsistencySummaryStatistics result = check();

            assertTrue(result.isConsistent(), result + System.lineSeparator() + logStream);
        }

        @ParameterizedTest
        @EnumSource(IndexSize.class)
        void shouldReportIndexInconsistencies(IndexSize indexSize) throws Exception {
            indexSize.createAdditionalData(fixture);

            NodeStore nodeStore = fixture.directStoreAccess().nativeStores().getNodeStore();
            StoreCursors storeCursors = fixture.getStoreCursors();
            try (var cursor = storeCursors.writeCursor(NODE_CURSOR)) {
                for (Long id : indexedNodes) {
                    NodeRecord nodeRecord = new NodeRecord(id);
                    nodeRecord.clear();
                    nodeStore.updateRecord(nodeRecord, cursor, NULL_CONTEXT, storeCursors);
                }
            }

            ConsistencySummaryStatistics stats = check();

            assertFalse(stats.isConsistent());
            assertThat(logStream.toString()).contains("This index entry refers to a node record that is not in use");
            assertThat(stats.getInconsistencyCountForRecordType(RecordType.INDEX.name()))
                    .isEqualTo(3);
        }

        @ParameterizedTest
        @EnumSource(IndexSize.class)
        void shouldReportNodesThatAreNotIndexed(IndexSize indexSize) throws Exception {
            indexSize.createAdditionalData(fixture);

            for (IndexDescriptor indexDescriptor : getValueIndexDescriptors()) {
                if (indexDescriptor.schema().entityType() == EntityType.NODE) {
                    IndexAccessor accessor = fixture.indexAccessorLookup().apply(indexDescriptor);
                    try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                        for (long nodeId : indexedNodes) {
                            EntityUpdates updates = fixture.nodeAsUpdates(nodeId);
                            for (IndexEntryUpdate<?> update :
                                    updates.valueUpdatesForIndexKeys(singletonList(indexDescriptor))) {
                                updater.process(IndexEntryUpdate.remove(
                                        nodeId, indexDescriptor, ((ValueIndexEntryUpdate<?>) update).values()));
                            }
                        }
                    }
                }
            }

            ConsistencySummaryStatistics stats = check();

            assertFalse(stats.isConsistent());
            assertThat(logStream.toString()).contains("This node was not found in the expected index");
            assertThat(stats.getInconsistencyCountForRecordType(RecordType.NODE.name()))
                    .isEqualTo(3);
        }

        // All the index types doesn't stores values and will not actually be tested by different checkers depending on
        // the size,
        // but doesn't hurt to run it for all anyway.
        @ParameterizedTest
        @EnumSource(IndexSize.class)
        void shouldReportRelationshipsThatAreNotIndexed(IndexSize indexSize) throws Exception {
            indexSize.createAdditionalData(fixture);

            for (IndexDescriptor indexDescriptor : getValueIndexDescriptors()) {
                if (indexDescriptor.schema().entityType() == EntityType.RELATIONSHIP) {
                    IndexAccessor accessor = fixture.indexAccessorLookup().apply(indexDescriptor);
                    try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                        for (long relId : indexedRelationships) {
                            EntityUpdates updates = fixture.relationshipAsUpdates(relId);
                            for (IndexEntryUpdate<?> update :
                                    updates.valueUpdatesForIndexKeys(singletonList(indexDescriptor))) {
                                updater.process(IndexEntryUpdate.remove(
                                        relId, indexDescriptor, ((ValueIndexEntryUpdate<?>) update).values()));
                            }
                        }
                    }
                }
            }

            ConsistencySummaryStatistics stats = check();

            assertFalse(stats.isConsistent());
            assertThat(logStream.toString()).contains("This relationship was not found in the expected index");
            assertThat(stats.getInconsistencyCountForRecordType(RecordType.RELATIONSHIP.name()))
                    .isEqualTo(3);
        }

        @ParameterizedTest
        @EnumSource(IndexSize.class)
        void shouldReportNodesThatAreIndexedWhenTheyShouldNotBe(IndexSize indexSize) throws Exception {
            indexSize.createAdditionalData(fixture);

            // given
            long newNode = createOneNode();

            Iterable<IndexDescriptor> indexDescriptors = getValueIndexDescriptors();
            for (IndexDescriptor indexDescriptor : indexDescriptors) {
                if (indexDescriptor.schema().entityType() == EntityType.NODE && !indexDescriptor.isUnique()) {
                    IndexAccessor accessor = fixture.indexAccessorLookup().apply(indexDescriptor);
                    try (IndexUpdater updater = accessor.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                        updater.process(IndexEntryUpdate.add(newNode, indexDescriptor, values(indexDescriptor)));
                    }
                }
            }

            // when
            ConsistencySummaryStatistics stats = check();

            assertFalse(stats.isConsistent());
            assertThat(stats.getInconsistencyCountForRecordType(RecordType.INDEX.name()))
                    .isEqualTo(2);
        }

        Value[] values(IndexDescriptor indexRule) {
            return switch (indexRule.schema().getPropertyIds().length) {
                case 1 -> Iterators.array(Values.of(indexedValue()));
                case 2 -> Iterators.array(Values.of(indexedValue()), Values.of(anotherIndexedValue()));
                default -> throw new UnsupportedOperationException();
            };
        }

        private Iterable<IndexDescriptor> getValueIndexDescriptors() {
            return fixture.getIndexDescriptors().stream()
                    .filter(descriptor -> !descriptor.isTokenIndex())
                    .toList();
        }

        private ConsistencySummaryStatistics check() throws ConsistencyCheckIncompleteException {
            // the database must not be running during the check because of Lucene-based indexes
            // Lucene files are locked when the DB is running
            fixture.close();

            var config = Config.newBuilder()
                    .set(GraphDatabaseSettings.neo4j_home, testDirectory.homePath())
                    .set(settings)
                    .build();
            return new ConsistencyCheckService(Neo4jLayout.of(config).databaseLayout("neo4j"))
                    .with(config)
                    .with(logProvider)
                    .runFullConsistencyCheck()
                    .summary();
        }

        private GraphStoreFixture createFixture() {
            return new GraphStoreFixture(testDirectory) {
                @Override
                protected void generateInitialData(GraphDatabaseService db) {
                    try (var tx = db.beginTx()) {
                        createNodeIndex(tx, PROP1);
                        createNodeIndex(tx, PROP2);

                        createRelIndex(tx, PROP1);
                        createRelIndex(tx, PROP2);
                        tx.commit();
                    }
                    try (var tx = db.beginTx()) {
                        tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
                    }

                    // Create initial data
                    try (org.neo4j.graphdb.Transaction tx = db.beginTx()) {
                        Node node1 = set(tx.createNode(INDEXED_LABEL), property(PROP1, indexedValue()));
                        Node node2 = set(
                                tx.createNode(INDEXED_LABEL),
                                property(PROP1, indexedValue()),
                                property(PROP2, anotherIndexedValue()));
                        Node node3 = set(tx.createNode(INDEXED_LABEL), property(PROP1, notIndexedValue()));
                        set(tx.createNode(label("AnotherLabel")), property(PROP1, indexedValue()));
                        set(tx.createNode(INDEXED_LABEL), property("anotherProperty", indexedValue()));
                        Node node6 = tx.createNode();

                        indexedNodes.add(node1.getId());
                        indexedNodes.add(node2.getId());

                        // Add another node that is indexed so our tests removing an indexed entry actually run for both
                        // IndexSizes
                        set(
                                tx.createNode(INDEXED_LABEL),
                                property(PROP1, indexedValue()),
                                property(PROP2, anotherIndexedValue()));

                        indexedRelationships.add(
                                set(node1.createRelationshipTo(node6, INDEXED_TYPE), property(PROP1, indexedValue()))
                                        .getId());
                        indexedRelationships.add(set(
                                        node2.createRelationshipTo(node6, INDEXED_TYPE),
                                        property(PROP1, indexedValue()),
                                        property(PROP2, anotherIndexedValue()))
                                .getId());
                        set(node3.createRelationshipTo(node6, INDEXED_TYPE), property(PROP1, notIndexedValue()))
                                .getId();

                        // Add another relationship that is indexed so our tests removing an indexed entry actually run
                        // for both IndexSizes
                        set(
                                node1.createRelationshipTo(node3, INDEXED_TYPE),
                                property(PROP1, anotherIndexedValue()),
                                property(PROP2, indexedValue()));
                        tx.commit();
                    }
                }

                @Override
                protected Map<Setting<?>, Object> getConfig() {
                    return settings;
                }
            };
        }

        protected long createOneNode() {
            final AtomicLong id = new AtomicLong();
            fixture.apply(tx -> id.set(tx.createNode().getId()));
            return id.get();
        }
    }

    @Nested
    class PointIndex extends TestBase {

        @Override
        IndexType type() {
            return IndexType.POINT;
        }

        @Override
        Object indexedValue() {
            return Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 2);
        }

        @Override
        Object anotherIndexedValue() {
            return Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 1, 2, 3);
        }

        @Override
        Object notIndexedValue() {
            return "some string";
        }
    }

    abstract static class TextIndexBase extends TestBase {

        private final IndexProviderDescriptor descriptor;

        TextIndexBase(IndexProviderDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        IndexType type() {
            return IndexType.TEXT;
        }

        @Override
        IndexPrototype nodeIndex(KernelTransaction ktx, String propKey) throws KernelException {
            return super.nodeIndex(ktx, propKey).withIndexProvider(descriptor);
        }

        @Override
        IndexPrototype relIndex(KernelTransaction ktx, String propKey) throws KernelException {
            return super.relIndex(ktx, propKey).withIndexProvider(descriptor);
        }

        @Override
        Object indexedValue() {
            return "some text";
        }

        @Override
        Object anotherIndexedValue() {
            return "another piece of text";
        }

        @Override
        Object notIndexedValue() {
            return 123;
        }
    }

    @Nested
    class TextIndex extends TextIndexBase {
        TextIndex() {
            super(TextIndexProvider.DESCRIPTOR);
        }
    }

    @Nested
    class TrigramTextIndex extends TextIndexBase {
        TrigramTextIndex() {
            super(TrigramIndexProvider.DESCRIPTOR);
        }
    }

    @Nested
    class FullTextIndex extends TestBase {
        @Override
        SchemaDescriptor nodeIndexSchema(int labelId, int propKeyId) {
            return SchemaDescriptors.fulltext(EntityType.NODE, new int[] {labelId}, new int[] {propKeyId});
        }

        @Override
        SchemaDescriptor relIndexSchema(int relTypeId, int propKeyId) {
            return SchemaDescriptors.fulltext(EntityType.RELATIONSHIP, new int[] {relTypeId}, new int[] {propKeyId});
        }

        @Override
        IndexType type() {
            return IndexType.FULLTEXT;
        }

        @Override
        Object indexedValue() {
            return "some text";
        }

        @Override
        Object anotherIndexedValue() {
            return "another piece of text";
        }

        @Override
        Object notIndexedValue() {
            return 123;
        }
    }

    abstract static class VectorIndexBase extends TestBase {
        private static final int DIMENSIONS = 100;
        private static final String SIMILARITY_FUNCTION = "COSINE";
        private static final IndexConfig CONFIG = IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap(Map.of(
                IndexSetting.vector_Dimensions(),
                DIMENSIONS,
                IndexSetting.vector_Similarity_Function(),
                SIMILARITY_FUNCTION));
        private static final float[] valid1 = new float[DIMENSIONS];
        private static final Double[] valid2 = new Double[DIMENSIONS];
        private static final double[] invalid = new double[DIMENSIONS / 2];

        static {
            for (int i = 0; i < DIMENSIONS; i++) {
                valid1[i] = (float) i;
                valid2[i] = (double) (DIMENSIONS - i);
                if (i < invalid.length) {
                    invalid[i] = i / 2.;
                }
            }
        }

        private final IndexProviderDescriptor descriptor;

        VectorIndexBase(VectorIndexVersion vectorIndexVersion) {
            this.descriptor = vectorIndexVersion.descriptor();
        }

        @Override
        IndexType type() {
            return IndexType.VECTOR;
        }

        @Override
        Object indexedValue() {
            return valid1;
        }

        @Override
        Object anotherIndexedValue() {
            return valid2;
        }

        @Override
        Object notIndexedValue() {
            return invalid;
        }

        @Override
        IndexPrototype nodeIndex(KernelTransaction ktx, String propKey) throws KernelException {
            return super.nodeIndex(ktx, propKey).withIndexProvider(descriptor).withIndexConfig(CONFIG);
        }

        @Override
        IndexPrototype relIndex(KernelTransaction ktx, String propKey) {
            // relationship vector index is unsupported
            return null;
        }

        @Override
        @Disabled("relationship vector index is unsupported")
        @ParameterizedTest
        @EnumSource(IndexSize.class)
        void shouldReportRelationshipsThatAreNotIndexed(IndexSize indexSize) {}
    }

    @Nested
    class VectorV1Index extends VectorIndexBase {
        VectorV1Index() {
            super(VectorIndexVersion.V1_0);
        }
    }

    @Nested
    class VectorV2Index extends VectorIndexBase {
        private static final DbmsRuntimeVersion LATEST_RUNTIME_VERSION = DbmsRuntimeVersion.GLORIOUS_FUTURE;
        private static final KernelVersion LATEST_KERNEL_VERSION = LATEST_RUNTIME_VERSION.kernelVersion();

        VectorV2Index() {
            super(VectorIndexVersion.V2_0);
            settings.put(GraphDatabaseInternalSettings.enable_vector_2, true);
            settings.put(GraphDatabaseInternalSettings.latest_runtime_version, LATEST_RUNTIME_VERSION.getVersion());
            settings.put(GraphDatabaseInternalSettings.latest_kernel_version, LATEST_KERNEL_VERSION.version());
        }
    }

    /**
     * Indexes are consistency checked in different ways depending on their size.
     * This can be used to make the indexes created in the setup appear large or small.
     */
    private enum IndexSize {
        SMALL_INDEX {
            @Override
            public void createAdditionalData(GraphStoreFixture fixture) {
                fixture.apply(tx -> {
                    // Create more nodes/relationships so our indexes will be considered to be small indexes
                    // (less than 5% of nodes/relationships in index).
                    for (int i = 0; i < 80; i++) {
                        Node node = tx.createNode();
                        node.createRelationshipTo(node, withName("OtherType"));
                    }
                });
            }
        },
        LARGE_INDEX {
            @Override
            public void createAdditionalData(GraphStoreFixture fixture) {}
        };

        public abstract void createAdditionalData(GraphStoreFixture fixture);
    }
}
