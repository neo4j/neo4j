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
package org.neo4j.kernel.api.impl.fulltext;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.fulltextSearch;
import static org.neo4j.internal.schema.IndexType.FULLTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.config;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.FULLTEXT_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asNodeLabelStr;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asPropertiesStrList;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asRelationshipTypeStr;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.assertQueryFindsIds;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.MutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.FulltextSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.internal.recordstorage.SchemaStorage;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.newapi.ExtendedNodeValueIndexCursorAdapter;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@DbmsExtension
class FulltextIndexProviderTest {
    private static final String NAME = "fulltext";

    @Inject
    DbmsController controller;

    @Inject
    GraphDatabaseAPI db;

    @Inject
    KernelImpl kernel;

    @Inject
    FileSystemAbstraction fileSystem;

    private Node node1;
    private Node node2;
    private int labelIdHej;
    private int labelIdHa;
    private int labelIdHe;
    private int propIdHej;
    private int propIdHa;
    private int propIdHe;
    private int propIdHo;
    private String firstNodeId;
    private String firstRelationshipId;

    @BeforeEach
    void prepDB() {
        Label hej = label("hej");
        Label ha = label("ha");
        Label he = label("he");
        RelationshipType hejType = RelationshipType.withName("hej");
        try (Transaction transaction = db.beginTx()) {
            node1 = transaction.createNode(hej, ha, he);
            node1.setProperty("hej", "value");
            node1.setProperty("ha", "value1");
            node1.setProperty("he", "value2");
            node1.setProperty("ho", "value3");
            firstNodeId = node1.getElementId();
            node2 = transaction.createNode();
            Relationship rel = node1.createRelationshipTo(node2, hejType);
            rel.setProperty("hej", "valuuu");
            rel.setProperty("ha", "value1");
            rel.setProperty("he", "value2");
            rel.setProperty("ho", "value3");
            firstRelationshipId = rel.getElementId();

            transaction.commit();
        }

        try (Transaction tx = db.beginTx()) {
            TokenRead tokenRead = tokenRead(tx);
            labelIdHej = tokenRead.nodeLabel(hej.name());
            labelIdHa = tokenRead.nodeLabel(ha.name());
            labelIdHe = tokenRead.nodeLabel(he.name());
            propIdHej = tokenRead.propertyKey("hej");
            propIdHa = tokenRead.propertyKey("ha");
            propIdHe = tokenRead.propertyKey("he");
            propIdHo = tokenRead.propertyKey("ho");
            tx.commit();
        }
    }

    @Test
    void createFulltextIndex() throws Exception {
        IndexDescriptor fulltextIndex =
                createIndex(new int[] {labelIdHej, labelIdHa, labelIdHe}, new int[] {propIdHej, propIdHa, propIdHe});
        try (KernelTransactionImplementation transaction = getKernelTransaction()) {
            IndexDescriptor descriptor = transaction.schemaRead().indexGetForName(NAME);
            assertEquals(descriptor.schema(), fulltextIndex.schema());
        }
    }

    @Test
    void shouldHaveAReasonableDirectoryStructure() throws Exception {
        createIndex(new int[] {labelIdHej, labelIdHa, labelIdHe}, new int[] {propIdHej, propIdHa, propIdHe});
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            tx.commit();
        }
        try (KernelTransactionImplementation transaction = getKernelTransaction()) {
            IndexDescriptor descriptor = transaction.schemaRead().indexGetForName(NAME);
            Path indexDir = Path.of(
                    db.databaseLayout().databaseDirectory().toAbsolutePath().toString(),
                    "schema",
                    "index",
                    descriptor.getIndexProvider().name(),
                    "" + descriptor.getId());
            List<Path> listFiles = List.of(requireNonNull(FileUtils.listPaths(indexDir)));
            assertTrue(listFiles.contains(indexDir.resolve("failure-message")));
            assertTrue(listFiles.contains(indexDir.resolve("1")));
            assertTrue(listFiles.contains(indexDir.resolve(indexDir.getFileName() + ".tx")));
        }
    }

    @Test
    void createAndRetainFulltextIndex() throws Exception {
        IndexDescriptor fulltextIndex =
                createIndex(new int[] {labelIdHej, labelIdHa, labelIdHe}, new int[] {propIdHej, propIdHa, propIdHe});
        controller.restartDbms();
        verifyThatFulltextIndexIsPresent(fulltextIndex);
    }

    @Test
    void createAndRetainRelationshipFulltextIndex() throws Exception {
        IndexDescriptor indexReference;
        try (KernelTransactionImplementation transaction = getKernelTransaction()) {
            SchemaDescriptor schema = SchemaDescriptors.fulltext(
                    EntityType.RELATIONSHIP,
                    new int[] {labelIdHej, labelIdHa, labelIdHe},
                    new int[] {propIdHej, propIdHa, propIdHe, propIdHo});
            IndexPrototype prototype = IndexPrototype.forSchema(schema, AllIndexProviderDescriptors.FULLTEXT_DESCRIPTOR)
                    .withIndexType(FULLTEXT)
                    .withName("fulltext");
            indexReference = transaction.schemaWrite().indexCreate(prototype);
            transaction.commit();
        }
        await(indexReference);
        controller.restartDbms();

        verifyThatFulltextIndexIsPresent(indexReference);
    }

    @Test
    void createAndQueryFulltextIndex() throws Exception {
        IndexDescriptor indexReference;
        indexReference = createIndex(
                new int[] {labelIdHej, labelIdHa, labelIdHe}, new int[] {propIdHej, propIdHa, propIdHe, propIdHo});
        await(indexReference);
        String thirdNodeId = createTheThirdNode();
        verifyNodeData(thirdNodeId);
        controller.restartDbms();
        verifyNodeData(thirdNodeId);
    }

    @Test
    void createAndQueryFulltextRelationshipIndex() throws Exception {
        IndexDescriptor indexReference;
        try (KernelTransactionImplementation transaction = getKernelTransaction()) {
            SchemaDescriptor schema = SchemaDescriptors.fulltext(
                    EntityType.RELATIONSHIP,
                    new int[] {labelIdHej, labelIdHa, labelIdHe},
                    new int[] {propIdHej, propIdHa, propIdHe, propIdHo});
            IndexPrototype prototype = IndexPrototype.forSchema(schema, AllIndexProviderDescriptors.FULLTEXT_DESCRIPTOR)
                    .withIndexType(FULLTEXT)
                    .withName("fulltext");
            indexReference = transaction.schemaWrite().indexCreate(prototype);
            transaction.commit();
        }
        await(indexReference);
        String secondRelId;
        try (Transaction transaction = db.beginTx()) {
            Relationship ho = transaction
                    .getNodeByElementId(node1.getElementId())
                    .createRelationshipTo(
                            transaction.getNodeByElementId(node2.getElementId()), RelationshipType.withName("ho"));
            secondRelId = ho.getElementId();
            ho.setProperty("hej", "villa");
            ho.setProperty("ho", "value3");
            transaction.commit();
        }
        verifyRelationshipData(secondRelId);
        controller.restartDbms();
        verifyRelationshipData(secondRelId);
    }

    @Test
    void multiTokenFulltextIndexesMustShowUpInSchemaGetIndexes() {
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(
                            FULLTEXT_CREATE,
                            "nodeIndex",
                            asNodeLabelStr("Label1", "Label2"),
                            asPropertiesStrList("prop1", "prop2")))
                    .close();
            tx.execute(format(
                            FULLTEXT_CREATE,
                            "relIndex",
                            asRelationshipTypeStr("RelType1", "RelType2"),
                            asPropertiesStrList("prop1", "prop2")))
                    .close();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            for (IndexDefinition index : tx.schema().getIndexes()) {
                if (index.getIndexType() == org.neo4j.graphdb.schema.IndexType.LOOKUP) {
                    continue;
                }
                assertFalse(index.isConstraintIndex());
                assertTrue(index.isMultiTokenIndex());
                assertTrue(index.isCompositeIndex());
                if (index.isNodeIndex()) {
                    assertFalse(index.isRelationshipIndex());
                    assertThat(index.getLabels()).contains(Label.label("Label1"), Label.label("Label2"));
                    try {
                        index.getRelationshipTypes();
                        fail("index.getRelationshipTypes() on node IndexDefinition should have thrown.");
                    } catch (IllegalStateException ignore) {
                    }
                } else {
                    assertTrue(index.isRelationshipIndex());
                    assertThat(index.getRelationshipTypes())
                            .contains(RelationshipType.withName("RelType1"), RelationshipType.withName("RelType2"));
                    try {
                        index.getLabels();
                        fail("index.getLabels() on node IndexDefinition should have thrown.");
                    } catch (IllegalStateException ignore) {
                    }
                }
            }
            tx.commit();
        }
    }

    @Test
    void awaitIndexesOnlineMustWorkOnFulltextIndexes() {
        String prop1 = "prop1";
        String prop2 = "prop2";
        String prop3 = "prop3";
        String val1 = "foo foo";
        String val2 = "bar bar";
        String val3 = "baz baz";
        Label label1 = Label.label("FirstLabel");
        Label label2 = Label.label("SecondLabel");
        Label label3 = Label.label("ThirdLabel");
        RelationshipType relType1 = RelationshipType.withName("FirstRelType");
        RelationshipType relType2 = RelationshipType.withName("SecondRelType");
        RelationshipType relType3 = RelationshipType.withName("ThirdRelType");

        MutableSet<String> nodes1 = Sets.mutable.empty();
        MutableSet<String> nodes2 = Sets.mutable.empty();
        MutableSet<String> nodes3 = Sets.mutable.empty();
        MutableSet<String> rels1 = Sets.mutable.empty();
        MutableSet<String> rels2 = Sets.mutable.empty();
        MutableSet<String> rels3 = Sets.mutable.empty();

        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 100; i++) {
                Node node1 = tx.createNode(label1);
                node1.setProperty(prop1, val1);
                nodes1.add(node1.getElementId());
                Relationship rel1 = node1.createRelationshipTo(node1, relType1);
                rel1.setProperty(prop1, val1);
                rels1.add(rel1.getElementId());

                Node node2 = tx.createNode(label2);
                node2.setProperty(prop2, val2);
                nodes2.add(node2.getElementId());
                Relationship rel2 = node1.createRelationshipTo(node2, relType2);
                rel2.setProperty(prop2, val2);
                rels2.add(rel2.getElementId());

                Node node3 = tx.createNode(label3);
                node3.setProperty(prop3, val3);
                nodes3.add(node3.getElementId());
                Relationship rel3 = node1.createRelationshipTo(node3, relType3);
                rel3.setProperty(prop3, val3);
                rels3.add(rel3.getElementId());
            }
            tx.commit();
        }

        // Test that multi-token node indexes can be waited for.
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(
                            FULLTEXT_CREATE,
                            "nodeIndex",
                            asNodeLabelStr(label1.name(), label2.name(), label3.name()),
                            asPropertiesStrList(prop1, prop2, prop3)))
                    .close();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(30, TimeUnit.SECONDS);
            tx.commit();
        }

        assertQueryFindsIds(db, true, "nodeIndex", "foo", nodes1);
        assertQueryFindsIds(db, true, "nodeIndex", "bar", nodes2);
        assertQueryFindsIds(db, true, "nodeIndex", "baz", nodes3);

        // Test that multi-token relationship indexes can be waited for.
        try (Transaction tx = db.beginTx()) {
            tx.execute(format(
                            FULLTEXT_CREATE,
                            "relIndex",
                            asRelationshipTypeStr(relType1.name(), relType2.name(), relType3.name()),
                            asPropertiesStrList(prop1, prop2, prop3)))
                    .close();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(30, TimeUnit.SECONDS);
            tx.commit();
        }

        assertQueryFindsIds(db, false, "relIndex", "foo", rels1);
        assertQueryFindsIds(db, false, "relIndex", "bar", rels2);
        assertQueryFindsIds(db, false, "relIndex", "baz", rels3);
    }

    @Test
    void queryingWithIndexProgressorMustProvideScore() throws Exception {
        String nodeId = createTheThirdNode();
        IndexDescriptor index;
        index = createIndex(
                new int[] {labelIdHej, labelIdHa, labelIdHe}, new int[] {propIdHej, propIdHa, propIdHe, propIdHo});
        await(index);
        List<String> acceptedEntities = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            var idMapper = ((TransactionImpl) tx).elementIdMapper();
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction(tx);
            NodeValueIndexCursor cursor = new ExtendedNodeValueIndexCursorAdapter() {
                private long nodeReference;
                private IndexProgressor progressor;

                @Override
                public long nodeReference() {
                    return nodeReference;
                }

                @Override
                public boolean next() {
                    return progressor.next();
                }

                @Override
                public void initializeQuery(
                        IndexDescriptor descriptor,
                        IndexProgressor progressor,
                        boolean indexIncludesTransactionState,
                        boolean needStoreFilter,
                        IndexQueryConstraints constraints,
                        PropertyIndexQuery... query) {
                    this.progressor = progressor;
                }

                @Override
                public boolean acceptEntity(long reference, float score, Value... values) {
                    this.nodeReference = reference;
                    assertFalse(Float.isNaN(score), "score should not be NaN");
                    assertThat(score).as("score must be positive").isGreaterThan(0.0f);
                    acceptedEntities.add(
                            "reference = " + reference + ", score = " + score + ", " + Arrays.toString(values));
                    return true;
                }
            };
            Read read = ktx.dataRead();
            IndexReadSession indexSession = ktx.dataRead().indexReadSession(index);
            read.nodeIndexSeek(
                    ktx.queryContext(), indexSession, cursor, unconstrained(), fulltextSearch("hej:\"villa\""));
            int counter = 0;
            while (cursor.next()) {
                assertThat(idMapper.nodeElementId(cursor.nodeReference())).isEqualTo(nodeId);
                counter++;
            }
            assertThat(counter).isEqualTo(1);
            assertThat(acceptedEntities.size()).isEqualTo(1);
            acceptedEntities.clear();
        }
    }

    @Test
    void validateMustThrowIfSchemaIsNotFulltext() throws Exception {
        try (KernelTransactionImplementation transaction = getKernelTransaction()) {
            int[] propertyIds = {propIdHa};
            SchemaDescriptor schema = SchemaDescriptors.forLabel(labelIdHa, propertyIds);
            IndexPrototype prototype =
                    IndexPrototype.forSchema(schema).withIndexType(FULLTEXT).withName(NAME);
            SchemaWrite schemaWrite = transaction.schemaWrite();
            var e = assertThrows(IllegalArgumentException.class, () -> schemaWrite.indexCreate(prototype));
            assertThat(e.getMessage()).contains("schema is not a full-text index schema");
        }
    }

    @Test
    void indexWithUnknownAnalyzerWillBeMarkedAsFailedOnStartup() throws Exception {
        assumeThat(db.getDependencyResolver().resolveDependency(StorageEngineFactory.class))
                .isInstanceOf(RecordStorageEngineFactory.class);

        // Create a full-text index.
        long indexId;
        try (KernelTransactionImplementation transaction = getKernelTransaction()) {
            int[] propertyIds = {propIdHa};
            SchemaDescriptor schema = SchemaDescriptors.fulltext(EntityType.NODE, new int[] {labelIdHa}, propertyIds);
            IndexPrototype prototype =
                    IndexPrototype.forSchema(schema).withIndexType(FULLTEXT).withName(NAME);
            SchemaWrite schemaWrite = transaction.schemaWrite();
            IndexDescriptor index = schemaWrite.indexCreate(prototype);
            indexId = index.getId();
            transaction.commit();
        }

        // The population job must have started for the fulltext index directory to exist. If the directory doesn't
        // exist when starting up and finding the bogus analyzer we won't be able to create the failure message
        // file and the failure message will be "". Easiest thing is just waiting for the index to come online,
        // then it definitely exists.
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(30, TimeUnit.SECONDS);
            tx.commit();
        }

        // Modify the full-text index such that it has an analyzer configured that does not exist.
        controller.restartDbms(builder -> {
            var cacheTracer = NULL;
            CursorContextFactory contextFactory = new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER);
            RecordDatabaseLayout databaseLayout = RecordDatabaseLayout.of(
                    Config.defaults(GraphDatabaseSettings.neo4j_home, builder.getHomeDirectory()));
            DefaultIdGeneratorFactory idGenFactory = new DefaultIdGeneratorFactory(
                    fileSystem, RecoveryCleanupWorkCollector.ignore(), cacheTracer, databaseLayout.getDatabaseName());
            try (JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();
                    PageCache pageCache = StandalonePageCacheFactory.createPageCache(
                            fileSystem, scheduler, cacheTracer, config(100))) {

                StoreFactory factory = new StoreFactory(
                        databaseLayout,
                        Config.defaults(),
                        idGenFactory,
                        pageCache,
                        cacheTracer,
                        fileSystem,
                        NullLogProvider.getInstance(),
                        contextFactory,
                        false,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
                var cursorContext = CursorContext.NULL_CONTEXT;
                try (NeoStores neoStores = factory.openAllNeoStores();
                        var storeCursors = new CachedStoreCursors(neoStores, cursorContext)) {
                    TokenHolders tokens =
                            StoreTokens.readOnlyTokenHolders(neoStores, storeCursors, EmptyMemoryTracker.INSTANCE);
                    var allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
                    SchemaStore schemaStore = neoStores.getSchemaStore();
                    SchemaStorage storage = new SchemaStorage(schemaStore, tokens);
                    IndexDescriptor index = (IndexDescriptor)
                            storage.loadSingleSchemaRule(indexId, storeCursors, EmptyMemoryTracker.INSTANCE);
                    Map<String, Value> indexConfigMap =
                            new HashMap<>(index.getIndexConfig().asMap());
                    for (Map.Entry<String, Value> entry : indexConfigMap.entrySet()) {
                        if (entry.getKey().contains("analyzer")) {
                            entry.setValue(Values.stringValue("bla-bla-lyzer")); // This analyzer does not exist!
                        }
                    }
                    index = index.withIndexConfig(IndexConfig.with(indexConfigMap));
                    storage.writeSchemaRule(
                            index, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, INSTANCE, storeCursors);
                    schemaStore.flush(FileFlushEvent.NULL, cursorContext);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return builder;
        });

        // Verify that the index comes up in a failed state.
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema().getIndexByName(NAME);

            Schema.IndexState indexState = tx.schema().getIndexState(index);
            assertThat(indexState).isEqualTo(Schema.IndexState.FAILED);

            String indexFailure = tx.schema().getIndexFailure(index);
            assertThat(indexFailure).contains("bla-bla-lyzer");
        }

        // Verify that the failed index can be dropped.
        try (Transaction tx = db.beginTx()) {
            tx.schema().getIndexByName(NAME).drop();
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName(NAME));
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName(NAME));
        }
        controller.restartDbms();
        try (Transaction tx = db.beginTx()) {
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName(NAME));
        }
    }

    @ResourceLock("BrokenAnalyzerProvider")
    @Test
    void indexWithAnalyzerThatThrowsWillNotBeCreated() {
        BrokenAnalyzerProvider.shouldThrow = true;
        BrokenAnalyzerProvider.shouldReturnNull = false;
        try (Transaction tx = db.beginTx()) {
            IndexCreator creator = tx.schema()
                    .indexFor(label("Label"))
                    .withIndexType(org.neo4j.graphdb.schema.IndexType.FULLTEXT)
                    .withIndexConfiguration(Map.of(IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME))
                    .on("prop")
                    .withName(NAME);

            // Validation must initially prevent this index from being created.
            var e = assertThrows(RuntimeException.class, creator::create);
            assertThat(e.getMessage()).contains("boom");

            // Create the index anyway.
            BrokenAnalyzerProvider.shouldThrow = false;
            creator.create();
            BrokenAnalyzerProvider.shouldThrow = true;

            // The analyzer will now throw during the index population, and the index should then enter a FAILED state.
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            var e = assertThrows(
                    IllegalStateException.class, () -> tx.schema().awaitIndexOnline(NAME, 10, TimeUnit.SECONDS));
            assertThat(e.getMessage()).contains("FAILED");
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            assertThat(tx.schema().getIndexState(index)).isEqualTo(Schema.IndexState.FAILED);
            index.drop();
            tx.commit();
        }

        BrokenAnalyzerProvider.shouldThrow = false;
        try (Transaction tx = db.beginTx()) {
            IndexCreator creator = tx.schema()
                    .indexFor(label("Label"))
                    .withIndexType(org.neo4j.graphdb.schema.IndexType.FULLTEXT)
                    .withIndexConfiguration(Map.of(IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME))
                    .on("prop")
                    .withName(NAME);

            // The analyzer no longer throws.
            creator.create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(NAME, 1, TimeUnit.MINUTES);
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            Schema.IndexState indexState = tx.schema().getIndexState(index);
            assertThat(indexState).isEqualTo(Schema.IndexState.ONLINE);
        }
        controller.restartDbms();
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            Schema.IndexState indexState = tx.schema().getIndexState(index);
            assertThat(indexState).isEqualTo(Schema.IndexState.ONLINE);
        }
    }

    @ResourceLock("BrokenAnalyzerProvider")
    @Test
    void indexWithAnalyzerThatReturnsNullWillNotBeCreated() {
        BrokenAnalyzerProvider.shouldThrow = false;
        BrokenAnalyzerProvider.shouldReturnNull = true;
        try (Transaction tx = db.beginTx()) {
            IndexCreator creator = tx.schema()
                    .indexFor(label("Label"))
                    .withIndexType(org.neo4j.graphdb.schema.IndexType.FULLTEXT)
                    .withIndexConfiguration(Map.of(IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME))
                    .on("prop")
                    .withName(NAME);

            // Validation must initially prevent this index from being created.
            var e = assertThrows(RuntimeException.class, creator::create);
            assertThat(e.getMessage()).contains("null");

            // Create the index anyway.
            BrokenAnalyzerProvider.shouldReturnNull = false;
            creator.create();
            BrokenAnalyzerProvider.shouldReturnNull = true;

            // The analyzer will now return null during the index population, and the index should then enter a FAILED
            // state.
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            var e = assertThrows(
                    IllegalStateException.class, () -> tx.schema().awaitIndexOnline(NAME, 1, TimeUnit.MINUTES));
            assertThat(e.getMessage()).contains("FAILED");
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            assertThat(tx.schema().getIndexState(index)).isEqualTo(Schema.IndexState.FAILED);
            index.drop();
            tx.commit();
        }

        BrokenAnalyzerProvider.shouldReturnNull = false;
        try (Transaction tx = db.beginTx()) {
            IndexCreator creator = tx.schema()
                    .indexFor(label("Label"))
                    .withIndexType(org.neo4j.graphdb.schema.IndexType.FULLTEXT)
                    .withIndexConfiguration(Map.of(IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME))
                    .on("prop")
                    .withName(NAME);

            // The analyzer no longer returns null.
            creator.create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(NAME, 1, TimeUnit.MINUTES);
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            Schema.IndexState indexState = tx.schema().getIndexState(index);
            assertThat(indexState).isEqualTo(Schema.IndexState.ONLINE);
        }
        controller.restartDbms();
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            Schema.IndexState indexState = tx.schema().getIndexState(index);
            assertThat(indexState).isEqualTo(Schema.IndexState.ONLINE);
        }
    }

    @ResourceLock("BrokenAnalyzerProvider")
    @Test
    void indexWithAnalyzerProviderThatThrowsAnExceptionOnStartupWillBeMarkedAsFailedOnStartup() {
        BrokenAnalyzerProvider.shouldThrow = false;
        BrokenAnalyzerProvider.shouldReturnNull = false;
        try (Transaction tx = db.beginTx()) {
            IndexCreator creator = tx.schema()
                    .indexFor(label("Label"))
                    .withIndexType(org.neo4j.graphdb.schema.IndexType.FULLTEXT)
                    .withIndexConfiguration(Map.of(IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME))
                    .on("prop")
                    .withName(NAME);

            // The analyzer no longer throws.
            creator.create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(NAME, 1, TimeUnit.MINUTES);
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            Schema.IndexState indexState = tx.schema().getIndexState(index);
            assertThat(indexState).isEqualTo(Schema.IndexState.ONLINE);
        }

        BrokenAnalyzerProvider.shouldThrow = true;
        controller.restartDbms();
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            Schema.IndexState indexState = tx.schema().getIndexState(index);
            assertThat(indexState).isEqualTo(Schema.IndexState.FAILED);
            String indexFailure = tx.schema().getIndexFailure(index);
            assertThat(indexFailure).contains("boom");
            index.drop();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName(NAME));
            tx.commit();
        }
    }

    @ResourceLock("BrokenAnalyzerProvider")
    @Test
    void indexWithAnalyzerProviderThatReturnsNullWillBeMarkedAsFailedOnStartup() {
        BrokenAnalyzerProvider.shouldThrow = false;
        BrokenAnalyzerProvider.shouldReturnNull = false;
        try (Transaction tx = db.beginTx()) {
            IndexCreator creator = tx.schema()
                    .indexFor(label("Label"))
                    .withIndexType(org.neo4j.graphdb.schema.IndexType.FULLTEXT)
                    .withIndexConfiguration(Map.of(IndexSetting.fulltext_Analyzer(), BrokenAnalyzerProvider.NAME))
                    .on("prop")
                    .withName(NAME);

            // The analyzer no longer returns null.
            creator.create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(NAME, 1, TimeUnit.MINUTES);
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            Schema.IndexState indexState = tx.schema().getIndexState(index);
            assertThat(indexState).isEqualTo(Schema.IndexState.ONLINE);
        }

        BrokenAnalyzerProvider.shouldReturnNull = true;
        controller.restartDbms();
        try (Transaction tx = db.beginTx()) {
            IndexDefinition index = tx.schema().getIndexByName(NAME);
            Schema.IndexState indexState = tx.schema().getIndexState(index);
            assertThat(indexState).isEqualTo(Schema.IndexState.FAILED);
            String indexFailure = tx.schema().getIndexFailure(index);
            assertThat(indexFailure).contains("null");
            index.drop();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            assertThrows(IllegalArgumentException.class, () -> tx.schema().getIndexByName(NAME));
            tx.commit();
        }
    }

    private static TokenRead tokenRead(Transaction tx) {
        return ((InternalTransaction) tx).kernelTransaction().tokenRead();
    }

    private KernelTransactionImplementation getKernelTransaction() {
        try {
            return (KernelTransactionImplementation)
                    kernel.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED);
        } catch (TransactionFailureException e) {
            throw new RuntimeException("oops");
        }
    }

    private IndexDescriptor createIndex(int[] entityTokens, int[] propertyIds) throws KernelException {
        return createIndex(entityTokens, propertyIds, FulltextSettings.fulltext_default_analyzer.defaultValue());
    }

    private IndexDescriptor createIndex(int[] entityTokens, int[] propertyIds, String analyzer) throws KernelException {
        return createIndex(entityTokens, propertyIds, analyzer, EntityType.NODE);
    }

    private IndexDescriptor createIndex(int[] entityTokens, int[] propertyIds, String analyzer, EntityType entityType)
            throws KernelException {
        return createIndex(entityTokens, propertyIds, analyzer, entityType, false);
    }

    private IndexDescriptor createIndex(
            int[] entityTokens, int[] propertyIds, String analyzer, EntityType entityType, boolean eventuallyConsistent)
            throws KernelException {
        IndexDescriptor fulltext;
        try (KernelTransactionImplementation transaction = getKernelTransaction()) {
            SchemaDescriptor schema = SchemaDescriptors.fulltext(entityType, entityTokens, propertyIds);
            IndexConfig config = IndexConfig.with(FulltextIndexSettingsKeys.ANALYZER, Values.stringValue(analyzer))
                    .withIfAbsent(FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT, Values.of(eventuallyConsistent));
            IndexPrototype prototype = IndexPrototype.forSchema(schema, AllIndexProviderDescriptors.FULLTEXT_DESCRIPTOR)
                    .withIndexType(IndexType.FULLTEXT)
                    .withName(NAME)
                    .withIndexConfig(config);
            fulltext = transaction.schemaWrite().indexCreate(prototype);
            transaction.commit();
        }
        return fulltext;
    }

    private void verifyThatFulltextIndexIsPresent(IndexDescriptor fulltextIndexDescriptor)
            throws TransactionFailureException {
        try (KernelTransactionImplementation transaction = getKernelTransaction()) {
            IndexDescriptor descriptor = transaction.schemaRead().indexGetForName(NAME);
            assertEquals(fulltextIndexDescriptor.schema(), descriptor.schema());
            assertEquals(fulltextIndexDescriptor.isUnique(), descriptor.isUnique());
        }
    }

    private String createTheThirdNode() {
        String nodeId;
        try (Transaction transaction = db.beginTx()) {
            Node hej = transaction.createNode(label("hej"));
            nodeId = hej.getElementId();
            hej.setProperty("hej", "villa");
            hej.setProperty("ho", "value3");
            transaction.commit();
        }
        return nodeId;
    }

    private void verifyNodeData(String thirdNodeId) throws Exception {
        try (Transaction tx = db.beginTx()) {
            var idMapper = ((TransactionImpl) tx).elementIdMapper();
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction(tx);
            IndexReadSession index =
                    ktx.dataRead().indexReadSession(ktx.schemaRead().indexGetForName("fulltext"));
            try (NodeValueIndexCursor cursor =
                    ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                assertIndexedNodes(idMapper, ktx, index, cursor, "value", firstNodeId);
                assertIndexedNodes(idMapper, ktx, index, cursor, "villa", thirdNodeId);
                assertIndexedNodes(idMapper, ktx, index, cursor, "value3", firstNodeId, thirdNodeId);
            }
            tx.commit();
        }
    }

    private void assertIndexedNodes(
            ElementIdMapper idMapper,
            KernelTransaction ktx,
            IndexReadSession index,
            NodeValueIndexCursor cursor,
            String query,
            String... nodeIds)
            throws KernelException {
        ktx.dataRead().nodeIndexSeek(ktx.queryContext(), index, cursor, unconstrained(), fulltextSearch(query));
        Set<String> found = new HashSet<>();
        while (cursor.next()) {
            found.add(idMapper.nodeElementId(cursor.nodeReference()));
        }
        assertThat(found).isEqualTo(asSet(nodeIds));
    }

    private void verifyRelationshipData(String secondRelId) throws Exception {
        try (Transaction tx = db.beginTx()) {
            var idMapper = ((TransactionImpl) tx).elementIdMapper();
            KernelTransaction ktx = LuceneFulltextTestSupport.kernelTransaction(tx);
            IndexDescriptor index = ktx.schemaRead().indexGetForName("fulltext");
            IndexReadSession indexReadSession = ktx.dataRead().indexReadSession(index);
            try (RelationshipValueIndexCursor cursor =
                    ktx.cursors().allocateRelationshipValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                assertIndexedRelationships(idMapper, ktx, indexReadSession, cursor, "valuuu", firstRelationshipId);
                assertIndexedRelationships(idMapper, ktx, indexReadSession, cursor, "villa", secondRelId);
                assertIndexedRelationships(
                        idMapper, ktx, indexReadSession, cursor, "value3", firstRelationshipId, secondRelId);
            }
            tx.commit();
        }
    }

    private void assertIndexedRelationships(
            ElementIdMapper idMapper,
            KernelTransaction ktx,
            IndexReadSession indexReadSession,
            RelationshipValueIndexCursor cursor,
            String query,
            String... relationshipIds)
            throws KernelException {
        ktx.dataRead()
                .relationshipIndexSeek(
                        ktx.queryContext(), indexReadSession, cursor, unconstrained(), fulltextSearch(query));
        Set<String> found = new HashSet<>();
        while (cursor.next()) {
            found.add(idMapper.relationshipElementId(cursor.relationshipReference()));
        }
        assertThat(found).isEqualTo(asSet(relationshipIds));
    }

    private void await(IndexDescriptor index) {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(index.getName(), 1, TimeUnit.MINUTES);
        }
    }
}
