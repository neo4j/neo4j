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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.FULLTEXT_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.FULLTEXT_CREATE_WITH_CONFIG;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asNodeLabelStr;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asPropertiesStrList;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asRelationshipTypeStr;
import static org.neo4j.kernel.api.impl.schema.fulltext.FulltextIndexSettingsKeys.ANALYZER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.MutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DbmsExtension(configurationCallback = "configure")
class FulltextProceduresTestSupport {
    static final String SCORE = "score";
    static final String NODE = "node";
    static final String RELATIONSHIP = "relationship";
    static final String DESCARTES_MEDITATIONES = "/meditationes--rene-descartes--public-domain.txt";
    static final Label LABEL = Label.label("Label");
    static final RelationshipType REL = RelationshipType.withName("REL");
    static final String PROP = "prop";
    static final String PROP2 = "prop2";
    static final String EVENTUALLY_CONSISTENT_OPTIONS = "{`fulltext.eventually_consistent`: true}";
    static final String QUERY_NODES = "CALL db.index.fulltext.queryNodes(\"%s\", \"%s\")";
    static final String QUERY_RELS = "CALL db.index.fulltext.queryRelationships(\"%s\", \"%s\")";
    static final String DEFAULT_NODE_IDX_NAME = "nodes";
    static final String DEFAULT_REL_IDX_NAME = "rels";

    @Inject
    GraphDatabaseAPI db;

    @Inject
    DbmsController controller;

    AtomicBoolean trapPopulation;
    Barrier.Control populationScanFinished;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        Monitors monitors = new Monitors();
        IndexMonitor.MonitorAdapter trappingMonitor = new IndexMonitor.MonitorAdapter() {
            @Override
            public void indexPopulationScanComplete(IndexDescriptor[] indexDescriptors) {
                if (trapPopulation.get()) {
                    populationScanFinished.reached();
                }
            }
        };
        monitors.addMonitorListener(trappingMonitor);
        builder.setMonitors(monitors);
        builder.setConfig(GraphDatabaseInternalSettings.always_use_latest_index_provider, false);
    }

    @BeforeEach
    void beforeEach() {
        trapPopulation = new AtomicBoolean();
        populationScanFinished = new Barrier.Control();
        GraphDatabaseServiceCleaner.cleanDatabaseContent(db);
    }

    static void assertNoIndexSeeks(Result result) {
        assertThat(result.stream().count()).isEqualTo(1L);
        String planDescription = result.getExecutionPlanDescription().toString();
        assertThat(planDescription).contains("NodeByLabel");
        assertThat(planDescription).doesNotContain("IndexSeek");
    }

    void restartDatabase() {
        controller.restartDbms(db.databaseName());
    }

    void awaitIndexesOnline() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            tx.commit();
        }
    }

    static void assertQueryFindsIdsInOrder(
            GraphDatabaseService db, boolean queryNodes, String index, String query, String... ids) {
        try (Transaction tx = db.beginTx()) {
            assertQueryFindsIdsInOrder(tx, queryNodes, index, query, ids);
            tx.commit();
        }
    }

    static void assertQueryFindsIdsInOrder(
            Transaction tx, boolean queryNodes, String index, String query, String... ids) {
        String queryCall = queryNodes ? QUERY_NODES : QUERY_RELS;
        Result result = tx.execute(format(queryCall, index, query));
        int num = 0;
        Double score = Double.MAX_VALUE;
        while (result.hasNext()) {
            Map<String, Object> entry = result.next();
            String nextId = ((Entity) entry.get(queryNodes ? NODE : RELATIONSHIP)).getElementId();
            Double nextScore = (Double) entry.get(SCORE);
            assertThat(nextScore).isLessThanOrEqualTo(score);
            score = nextScore;
            if (num < ids.length) {
                assertEquals(ids[num], nextId, format("Result returned id %s, expected %s", nextId, ids[num]));
            } else {
                fail(format(
                        "Result returned id %s, which is beyond the number of ids (%d) that were expected.",
                        nextId, ids.length));
            }
            num++;
        }
        assertEquals(ids.length, num, "Number of results differ from expected");
    }

    static void assertQueryFindsIds(
            GraphDatabaseService db, boolean queryNodes, String index, String query, Set<String> ids) {
        ids = Sets.mutable.withAll(ids); // Create a defensive copy, because we're going to modify this
        // instance.
        String queryCall = queryNodes ? QUERY_NODES : QUERY_RELS;
        String[] expectedIds = ids.toArray(String[]::new);
        MutableSet<String> actualIds = Sets.mutable.empty();
        try (Transaction tx = db.beginTx()) {
            Function<String, Entity> getEntity = queryNodes ? tx::getNodeByElementId : tx::getRelationshipByElementId;
            Result result = tx.execute(format(queryCall, index, query));
            Double score = Double.MAX_VALUE;
            while (result.hasNext()) {
                Map<String, Object> entry = result.next();
                String nextId = ((Entity) entry.get(queryNodes ? NODE : RELATIONSHIP)).getElementId();
                Double nextScore = (Double) entry.get(SCORE);
                assertThat(nextScore).isLessThanOrEqualTo(score);
                score = nextScore;
                actualIds.add(nextId);
                if (!ids.remove(nextId)) {
                    String msg = "This id was not expected: " + nextId;
                    failQuery(getEntity, index, query, ids, expectedIds, actualIds, msg);
                }
            }
            if (!ids.isEmpty()) {
                String msg = "Not all expected ids were found: " + ids;
                failQuery(getEntity, index, query, ids, expectedIds, actualIds, msg);
            }
            tx.commit();
        }
    }

    static void failQuery(
            Function<String, Entity> getEntity,
            String index,
            String query,
            Set<String> ids,
            String[] expectedIds,
            MutableSet<String> actualIds,
            String msg) {
        StringBuilder message = new StringBuilder(msg).append('\n');
        var itr = ids.iterator();
        while (itr.hasNext()) {
            var id = itr.next();
            Entity entity = getEntity.apply(id);
            message.append('\t')
                    .append(entity)
                    .append(entity.getAllProperties())
                    .append('\n');
        }
        message.append("for query: '")
                .append(query)
                .append("'\nin index: ")
                .append(index)
                .append('\n');
        message.append("all expected ids: ")
                .append(Arrays.toString(expectedIds))
                .append('\n');
        message.append("actual ids: ").append(actualIds);
        itr = actualIds.iterator();
        while (itr.hasNext()) {
            var id = itr.next();
            Entity entity = getEntity.apply(id);
            message.append("\n\t").append(entity).append(entity.getAllProperties());
        }
        fail(message.toString());
    }

    static List<Value> generateRandomNonStringValues(RandomValues rng) {
        Predicate<Value> nonString = v -> v.valueGroup() != ValueGroup.TEXT && v.valueGroup() != ValueGroup.TEXT_ARRAY;
        return generateRandomValues(nonString, rng);
    }

    static List<Value> generateRandomSimpleValues(RandomValues rng) {
        EnumSet<ValueGroup> simpleTypes =
                EnumSet.of(ValueGroup.BOOLEAN, ValueGroup.BOOLEAN_ARRAY, ValueGroup.NUMBER, ValueGroup.NUMBER_ARRAY);
        return generateRandomValues(v -> simpleTypes.contains(v.valueGroup()), rng);
    }

    static List<Value> generateRandomValues(Predicate<Value> predicate, RandomValues generator) {
        int valuesToGenerate = 1000;
        List<Value> values = new ArrayList<>(valuesToGenerate);
        for (int i = 0; i < valuesToGenerate; i++) {
            Value value;
            do {
                value = generator.nextValue();
            } while (!predicate.test(value));
            values.add(value);
        }
        return values;
    }

    void createIndexAndWait(EntityUtil entityUtil) {
        try (Transaction tx = db.beginTx()) {
            entityUtil.createIndex(tx);
            tx.commit();
        }
        awaitIndexesOnline();
    }

    void createCompositeIndexAndWait(EntityUtil entityUtil) {
        try (Transaction tx = db.beginTx()) {
            entityUtil.createCompositeIndex(tx);
            tx.commit();
        }
        awaitIndexesOnline();
    }

    static void createSimpleRelationshipIndex(Transaction tx) {
        tx.execute(format(
                        FULLTEXT_CREATE,
                        DEFAULT_REL_IDX_NAME,
                        asRelationshipTypeStr(REL.name()),
                        asPropertiesStrList(PROP)))
                .close();
    }

    static void createSimpleNodesIndex(Transaction tx) {
        tx.execute(format(
                        FULLTEXT_CREATE,
                        DEFAULT_NODE_IDX_NAME,
                        asNodeLabelStr(LABEL.name()),
                        asPropertiesStrList(PROP)))
                .close();
    }

    static void createSimpleRelationshipIndexWithProvider(Transaction tx, IndexProviderDescriptor indexProvider)
            throws KernelException {
        assertThat(AllIndexProviderDescriptors.INDEX_TYPES.get(indexProvider))
                .as(
                        "`%s` should be a %s %s",
                        indexProvider, IndexType.FULLTEXT.name(), IndexProviderDescriptor.class.getSimpleName())
                .isEqualTo(IndexType.FULLTEXT);

        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        TokenWrite tokenWrite = ktx.tokenWrite();
        int typeId = tokenWrite.relationshipTypeGetOrCreateForName(REL.name());
        int propKeyId = tokenWrite.propertyKeyGetOrCreateForName(PROP);
        IndexPrototype prototype = IndexPrototype.forSchema(SchemaDescriptors.forSemanticSearch(
                        EntityType.RELATIONSHIP, new int[] {typeId}, new int[] {propKeyId}))
                .withIndexType(IndexType.FULLTEXT)
                .withIndexProvider(indexProvider)
                .withName(DEFAULT_REL_IDX_NAME);
        ktx.schemaWrite().indexCreate(prototype);
    }

    static void createSimpleNodesIndexWithProvider(Transaction tx, IndexProviderDescriptor indexProvider)
            throws KernelException {
        assertThat(AllIndexProviderDescriptors.INDEX_TYPES.get(indexProvider))
                .as(
                        "`%s` should be a %s %s",
                        indexProvider, IndexType.FULLTEXT.name(), IndexProviderDescriptor.class.getSimpleName())
                .isEqualTo(IndexType.FULLTEXT);

        KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
        TokenWrite tokenWrite = ktx.tokenWrite();
        int labelId = tokenWrite.labelGetOrCreateForName(LABEL.name());
        int propKeyId = tokenWrite.propertyKeyGetOrCreateForName(PROP);
        IndexPrototype prototype = IndexPrototype.forSchema(SchemaDescriptors.forSemanticSearch(
                        EntityType.NODE, new int[] {labelId}, new int[] {propKeyId}))
                .withIndexType(IndexType.FULLTEXT)
                .withIndexProvider(indexProvider)
                .withName(DEFAULT_NODE_IDX_NAME);
        ktx.schemaWrite().indexCreate(prototype);
    }

    interface EntityUtil {
        void createIndex(Transaction tx);

        void createIndexWithProvider(Transaction tx, IndexProviderDescriptor indexProvider) throws KernelException;

        void createIndexWithAnalyzer(Transaction tx, String analyzer);

        void createCompositeIndex(Transaction tx);

        String createEntityWithProperty(Transaction tx, Object propertyValue);

        String createEntityWithProperties(Transaction tx, Object propertyValue, Object property2Value);

        String createEntity(Transaction tx);

        void assertQueryFindsIdsInOrder(Transaction tx, String query, String... ids);

        void assertQueryFindsIds(GraphDatabaseAPI db, String query, Set<String> ids);

        default void assertQueryFindsIdsInOrder(GraphDatabaseAPI db, String query, String... ids) {
            try (var tx = db.beginTx()) {
                assertQueryFindsIdsInOrder(tx, query, ids);
                tx.commit();
            }
        }

        void deleteEntity(Transaction tx, String entityId);

        void dropIndex(Transaction tx);

        Result queryIndex(Transaction tx, String query);

        ResourceIterator<Entity> queryIndexWithOptions(Transaction tx, String query, String options);

        Entity getEntity(Transaction tx, String id);
    }

    static class NodeUtil implements EntityUtil {
        @Override
        public void createIndex(Transaction tx) {
            createSimpleNodesIndex(tx);
        }

        @Override
        public void createIndexWithProvider(Transaction tx, IndexProviderDescriptor indexProvider)
                throws KernelException {
            createSimpleNodesIndexWithProvider(tx, indexProvider);
        }

        @Override
        public void createIndexWithAnalyzer(Transaction tx, String analyzer) {
            tx.execute(format(
                            FULLTEXT_CREATE_WITH_CONFIG,
                            DEFAULT_NODE_IDX_NAME,
                            asNodeLabelStr(LABEL.name()),
                            asPropertiesStrList(PROP),
                            "{`" + ANALYZER + "`: \"" + analyzer + "\"}"))
                    .close();
        }

        @Override
        public void createCompositeIndex(Transaction tx) {
            tx.execute(format(
                            FULLTEXT_CREATE,
                            DEFAULT_NODE_IDX_NAME,
                            asNodeLabelStr(LABEL.name()),
                            asPropertiesStrList(PROP, PROP2)))
                    .close();
        }

        @Override
        public String createEntityWithProperty(Transaction tx, Object propertyValue) {
            Node node = tx.createNode(LABEL);
            node.setProperty(PROP, propertyValue);
            return node.getElementId();
        }

        @Override
        public String createEntityWithProperties(Transaction tx, Object propertyValue, Object property2Value) {
            Node node = tx.createNode(LABEL);
            node.setProperty(PROP, propertyValue);
            node.setProperty(PROP2, property2Value);
            return node.getElementId();
        }

        @Override
        public String createEntity(Transaction tx) {
            return tx.createNode(LABEL).getElementId();
        }

        @Override
        public void assertQueryFindsIdsInOrder(Transaction tx, String query, String... ids) {
            FulltextProceduresTestSupport.assertQueryFindsIdsInOrder(tx, true, DEFAULT_NODE_IDX_NAME, query, ids);
        }

        @Override
        public void assertQueryFindsIds(GraphDatabaseAPI db, String query, Set<String> ids) {
            FulltextProceduresTestSupport.assertQueryFindsIds(db, true, DEFAULT_NODE_IDX_NAME, query, ids);
        }

        @Override
        public void deleteEntity(Transaction tx, String entityId) {
            tx.getNodeByElementId(entityId).delete();
        }

        @Override
        public void dropIndex(Transaction tx) {
            tx.execute(format("DROP INDEX `%s`", DEFAULT_NODE_IDX_NAME));
        }

        @Override
        public Result queryIndex(Transaction tx, String query) {
            return tx.execute(format(QUERY_NODES, DEFAULT_NODE_IDX_NAME, query));
        }

        @Override
        public ResourceIterator<Entity> queryIndexWithOptions(Transaction tx, String query, String options) {
            return tx.execute(format(
                            "CALL db.index.fulltext.queryNodes(\"%s\", \"%s\", %s )",
                            DEFAULT_NODE_IDX_NAME, query, options))
                    .columnAs("node");
        }

        @Override
        public Entity getEntity(Transaction tx, String id) {
            return tx.getNodeByElementId(id);
        }

        @Override
        public String toString() {
            return "For node";
        }
    }

    static Stream<EntityUtil> entityTypeProvider() {
        return Stream.of(new NodeUtil(), new RelationshipUtil());
    }

    static class RelationshipUtil implements EntityUtil {
        @Override
        public void createIndex(Transaction tx) {
            createSimpleRelationshipIndex(tx);
        }

        @Override
        public void createIndexWithProvider(Transaction tx, IndexProviderDescriptor indexProvider)
                throws KernelException {
            createSimpleRelationshipIndexWithProvider(tx, indexProvider);
        }

        @Override
        public void createIndexWithAnalyzer(Transaction tx, String analyzer) {
            tx.execute(format(
                            FULLTEXT_CREATE_WITH_CONFIG,
                            DEFAULT_REL_IDX_NAME,
                            asRelationshipTypeStr(REL.name()),
                            asPropertiesStrList(PROP),
                            "{`" + ANALYZER + "`: \"" + analyzer + "\"}"))
                    .close();
        }

        @Override
        public void createCompositeIndex(Transaction tx) {
            tx.execute(format(
                            FULLTEXT_CREATE,
                            DEFAULT_REL_IDX_NAME,
                            asRelationshipTypeStr(REL.name()),
                            asPropertiesStrList(PROP, PROP2)))
                    .close();
        }

        @Override
        public String createEntityWithProperty(Transaction tx, Object propertyValue) {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo(node, REL);
            rel.setProperty(PROP, propertyValue);
            return rel.getElementId();
        }

        @Override
        public String createEntityWithProperties(Transaction tx, Object propertyValue, Object property2Value) {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo(node, REL);
            rel.setProperty(PROP, propertyValue);
            rel.setProperty(PROP2, property2Value);
            return rel.getElementId();
        }

        @Override
        public String createEntity(Transaction tx) {
            Node node = tx.createNode();
            return node.createRelationshipTo(node, REL).getElementId();
        }

        @Override
        public void assertQueryFindsIdsInOrder(Transaction tx, String query, String... ids) {
            FulltextProceduresTestSupport.assertQueryFindsIdsInOrder(tx, false, DEFAULT_REL_IDX_NAME, query, ids);
        }

        @Override
        public void assertQueryFindsIds(GraphDatabaseAPI db, String query, Set<String> ids) {
            FulltextProceduresTestSupport.assertQueryFindsIds(db, false, DEFAULT_REL_IDX_NAME, query, ids);
        }

        @Override
        public void deleteEntity(Transaction tx, String entityId) {
            tx.getRelationshipByElementId(entityId).delete();
        }

        @Override
        public void dropIndex(Transaction tx) {
            tx.execute(format("DROP INDEX `%s`", DEFAULT_REL_IDX_NAME));
        }

        @Override
        public Result queryIndex(Transaction tx, String query) {
            return tx.execute(format(QUERY_RELS, DEFAULT_REL_IDX_NAME, query));
        }

        @Override
        public ResourceIterator<Entity> queryIndexWithOptions(Transaction tx, String query, String options) {
            return tx.execute(format(
                            "CALL db.index.fulltext.queryRelationships(\"%s\", \"%s\", %s )",
                            DEFAULT_REL_IDX_NAME, query, options))
                    .columnAs("relationship");
        }

        @Override
        public Entity getEntity(Transaction tx, String id) {
            return tx.getRelationshipByElementId(id);
        }

        @Override
        public String toString() {
            return "For relationship";
        }
    }
}
