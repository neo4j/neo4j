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
package org.neo4j.kernel.impl.index.vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.EntityFilterPredicate;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.NearestNeighborsPredicate;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.index.vector.VectorSSFQueryResult.ResultList;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

@ImpermanentDbmsExtension(configurationCallback = "configure")
abstract class VectorSSFTestBase {
    protected static final EmbeddingHolder EMBEDDINGS;

    static {
        try {
            EMBEDDINGS = EmbeddingHolder.from("vector-test-embedding-kaggle-netflix-shows.txt");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Inject
    protected GraphDatabaseAPI db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        configureSSFTest(builder);
    }

    protected void configureSSFTest(TestDatabaseManagementServiceBuilder builder) {
        // no configuration necessary in the most subclasses, so make this concrete.
    }

    private static final int EF_CONSTRUCTION = 1000;
    private static final int K_NEAREST_NEIGHBORS = 10;
    protected static final VectorSimilarityFunction SIMILARITY_FUNCTION =
            LatestVersions.LATEST_VECTOR_INDEX_VERSION.similarityFunction("COSINE");

    static final String EMBEDDING_NAME = "abstract_embedding";
    static final Label LABEL_NODE_1 = Label.label("NodeLabel1");
    static final RelationshipType TYPE_REL_1 = RelationshipType.withName("RelLabel1");
    static final String VECTOR_INDEX_NAME = "abstract_embedding_ix";

    static Function<TokenRead, PropertyIndexQuery> allQuery(String propertyKey) {
        return tokenRead -> PropertyIndexQuery.all(tokenRead.propertyKey(propertyKey));
    }

    static Function<TokenRead, PropertyIndexQuery> existsQuery(String propertyKey) {
        return tokenRead -> PropertyIndexQuery.exists(tokenRead.propertyKey(propertyKey));
    }

    static Function<TokenRead, PropertyIndexQuery> notExistsQuery(String propertyKey) {
        return tokenRead -> PropertyIndexQuery.notExists(tokenRead.propertyKey(propertyKey));
    }

    static Function<TokenRead, PropertyIndexQuery> exactQuery(String propertyKey, Value propertyValue) {
        return tokenRead -> PropertyIndexQuery.exact(tokenRead.propertyKey(propertyKey), propertyValue);
    }

    static Function<TokenRead, PropertyIndexQuery> inSetQuery(String propertyKey, Value[] propertyValues) {
        return tokenRead -> PropertyIndexQuery.inSet(tokenRead.propertyKey(propertyKey), propertyValues);
    }

    static Function<TokenRead, PropertyIndexQuery> genericInSetQuery(String propertyKey, Value... values) {
        return tokenRead -> PropertyIndexQuery.inSet(tokenRead.propertyKey(propertyKey), values);
    }

    static Function<TokenRead, PropertyIndexQuery> inSetQuerySingleton(String propertyKey, Value propertyValue) {
        return tokenRead -> PropertyIndexQuery.inSet(tokenRead.propertyKey(propertyKey), new Value[] {propertyValue});
    }

    static Function<TokenRead, PropertyIndexQuery> rangeQuery(
            String propertyKey, Value propertyFrom, boolean fromInclusive, Value propertyTo, boolean toInclusive) {
        return tokenRead -> PropertyIndexQuery.range(
                tokenRead.propertyKey(propertyKey), propertyFrom, fromInclusive, propertyTo, toInclusive);
    }

    static final Function<? super VectorSSFQueryResult, Long> EXTRACT_ENTITY_ID = VectorSSFQueryResult::getEntityId;

    static final Function<? super VectorSSFQueryResult, Long> EXTRACT_ID =
            result -> result.<IntegralValue>getValue("id").longValue();

    static final Function<? super VectorSSFQueryResult, String> EXTRACT_NAME =
            result -> result.<TextValue>getValue("name").stringValue();

    static class EmbeddingHolder {
        private final float[][] embeddings;

        private EmbeddingHolder(float[][] embeddings) {
            this.embeddings = embeddings;
        }

        static EmbeddingHolder from(String resource) throws IOException {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(
                    Preconditions.requireNonNull(
                            VectorSSFTestBase.class.getResourceAsStream(resource), "Resource not found"),
                    StandardCharsets.UTF_8))) {
                String s = in.readLine();
                List<float[]> embeddings = new ArrayList<>();
                while (s != null) {
                    String[] tokens = s.split(",");
                    float[] embedding = new float[tokens.length];
                    for (int i = 0; i < embedding.length; i++) {
                        embedding[i] = Float.parseFloat(tokens[i].trim());
                    }
                    embeddings.add(embedding);
                    s = in.readLine();
                }
                return new EmbeddingHolder(embeddings.toArray(new float[embeddings.size()][]));
            }
        }

        int dimensions() {
            return dimensionsFor(0);
        }

        int dimensionsFor(int i) {
            return embeddings[i].length;
        }

        float[] get(int i) {
            return embeddings[i];
        }

        int count() {
            return embeddings.length;
        }
    }

    protected void createNodeVectorIndex(String name, int vectorDimension, String... onProperties) {
        createNodeVectorIndex(name, vectorDimension, config -> {}, onProperties);
    }

    protected void createNodeVectorIndex(
            String name, int vectorDimension, Consumer<VectorIndexSettings> modifySettings, String... onProperties) {

        try (Transaction tx = db.beginTx()) {
            IndexCreator creator = tx.schema().indexFor(LABEL_NODE_1);
            for (String onProperty : onProperties) {
                creator = creator.on(onProperty);
            }

            VectorIndexSettings indexSettings = VectorIndexSettings.create()
                    .withDimensions(vectorDimension)
                    .withSimilarityFunction(SIMILARITY_FUNCTION)
                    .withHnswEfConstruction(EF_CONSTRUCTION);
            modifySettings.accept(indexSettings);

            creator = creator.withIndexType(IndexType.VECTOR.toPublicApi())
                    .withIndexConfiguration(indexSettings.toMap())
                    .withName(name);
            creator.create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(name, 2, TimeUnit.MINUTES);
        }
    }

    protected void createRelationshipVectorIndex(String name, int vectorDimension, String... onProperties) {
        try (Transaction tx = db.beginTx()) {
            IndexCreator creator = tx.schema().indexFor(TYPE_REL_1);
            for (String onProperty : onProperties) {
                creator = creator.on(onProperty);
            }
            VectorIndexSettings indexSettings = VectorIndexSettings.create()
                    .withDimensions(vectorDimension)
                    .withSimilarityFunction(SIMILARITY_FUNCTION)
                    .withHnswEfConstruction(EF_CONSTRUCTION);
            creator = creator.withIndexType(IndexType.VECTOR.toPublicApi())
                    .withIndexConfiguration(indexSettings.toMap())
                    .withName(name);
            creator.create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(name, 2, TimeUnit.MINUTES);
        }
    }

    /**
     * Create a single node with the specified properties
     * @param properties for the created record
     */
    protected long createTestNode(Map<String, Object> properties) {
        long node;
        try (Transaction tx = db.beginTx()) {
            node = createTestNode(tx, properties);
            tx.commit();
        }
        return node;
    }

    /**
     * Create a single node with the specified properties
     * @param properties for the created record
     */
    protected static long createTestNode(Transaction tx, Map<String, Object> properties) {
        Node node = tx.createNode(LABEL_NODE_1);
        for (Entry<String, Object> prop : properties.entrySet()) {
            node.setProperty(prop.getKey(), prop.getValue());
        }
        return node.getId();
    }

    /**
     * Create a single relationship with the specified properties
     * @param properties for the created record
     */
    protected long createTestRelationship(Map<String, Object> properties) {
        long rel;
        try (Transaction tx = db.beginTx()) {
            rel = createTestRelationship(tx, properties);
            tx.commit();
        }
        return rel;
    }

    /**
     * Create a single relationship with the specified properties
     * @param properties for the created record
     */
    protected static long createTestRelationship(Transaction tx, Map<String, Object> properties) {
        Relationship relationship =
                tx.createNode(LABEL_NODE_1).createRelationshipTo(tx.createNode(LABEL_NODE_1), TYPE_REL_1);
        for (Entry<String, Object> prop : properties.entrySet()) {
            relationship.setProperty(prop.getKey(), prop.getValue());
        }
        return relationship.getId();
    }

    /**
     * Delete a single node with the specified properties
     */
    protected void deleteTestNode(String key, Object value) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.findNode(LABEL_NODE_1, key, value);
            node.delete();
            tx.commit();
        }
    }

    /**
     * Update a single node record with the specified properties
     * @param properties for the created record
     */
    protected void updateTestNode(String key, Object value, Map<String, Object> properties) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.findNode(LABEL_NODE_1, key, value);
            if (node != null) {
                for (Entry<String, Object> prop : properties.entrySet()) {
                    String propKey = prop.getKey();
                    Object propValue = prop.getValue();
                    if (propValue != null) {
                        node.setProperty(propKey, propValue);
                    } else {
                        node.removeProperty(propKey);
                    }
                }
            }
            tx.commit();
        }
    }

    /**
     * Scan the available index descriptors for the first (only) one of name/type
     * @param ktx transaction in which we are executing
     * @param indexType of an index to search for
     * @param name of an index to search for
     * @return the first matching descriptor
     */
    private static IndexDescriptor findIndexDescriptor(KernelTransaction ktx, IndexType indexType, String name)
            throws TestException {

        IndexDescriptor index = ktx.schemaRead().indexGetForName(name);
        if (index.getId() == -1 || !index.getIndexType().equals(indexType)) {
            throw new TestException("This is not the expected index");
        }
        return index;
    }

    @SafeVarargs
    final List<VectorSSFQueryResult> queryNodeIndex(
            String indexName,
            NearestNeighborsPredicate kNearestNeighboursPredicate,
            Function<TokenRead, PropertyIndexQuery>... queryFilters)
            throws Exception {
        return queryNodeIndex(
                indexName, kNearestNeighboursPredicate, PropertyIndexQuery.matchAllEntityFilter(), queryFilters);
    }

    @SafeVarargs
    final ResultList queryNodeIndex(
            String indexName,
            NearestNeighborsPredicate kNearestNeighboursPredicate,
            EntityFilterPredicate entityFilterPredicate,
            Function<TokenRead, PropertyIndexQuery>... queryFilters)
            throws Exception {
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (NodeValueIndexCursor indexCursor =
                            ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker());
                    PropertyCursor propertyCursor =
                            ktx.cursors().allocatePropertyCursor(ktx.cursorContext(), ktx.memoryTracker());
                    NodeCursor nodeCursor =
                            ktx.cursors().allocateNodeCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                Read read = ktx.dataRead();
                TokenRead tokenRead = ktx.tokenRead();
                QueryContext queryContext = ktx.queryContext();
                IndexReadSession session = read.indexReadSession(findIndexDescriptor(ktx, IndexType.VECTOR, indexName));
                PropertyIndexQuery[] queries = new PropertyIndexQuery[queryFilters.length + 2];
                queries[0] = kNearestNeighboursPredicate;
                queries[1] = entityFilterPredicate;
                for (int i = 0; i < queryFilters.length; i++) {
                    Function<TokenRead, PropertyIndexQuery> queryFilter = queryFilters[i];
                    queries[i + 2] = queryFilter == null ? null : queryFilter.apply(tokenRead);
                }
                read.nodeIndexSeek(queryContext, session, indexCursor, IndexQueryConstraints.unconstrained(), queries);

                ResultList results = new ResultList();
                while (indexCursor.next()) {
                    float score = indexCursor.score();
                    indexCursor.node(nodeCursor);
                    while (nodeCursor.next()) {
                        results.add(VectorSSFQueryResult.fromCursors(
                                score, nodeCursor, propertyCursor, tokenRead, Set.of(EMBEDDING_NAME)));
                    }
                }
                return results;
            }
        }
    }

    @SafeVarargs
    final ResultList queryRelationshipIndex(
            String indexName,
            NearestNeighborsPredicate kNearestNeighboursPredicate,
            EntityFilterPredicate entityFilterPredicate,
            Function<TokenRead, PropertyIndexQuery>... queryFilters)
            throws Exception {
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            try (RelationshipValueIndexCursor indexCursor = ktx.cursors()
                            .allocateRelationshipValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker());
                    PropertyCursor propertyCursor =
                            ktx.cursors().allocatePropertyCursor(ktx.cursorContext(), ktx.memoryTracker());
                    RelationshipScanCursor relCursor =
                            ktx.cursors().allocateRelationshipScanCursor(ktx.cursorContext(), ktx.memoryTracker())) {
                Read read = ktx.dataRead();
                TokenRead tokenRead = ktx.tokenRead();
                QueryContext queryContext = ktx.queryContext();
                IndexReadSession session = read.indexReadSession(findIndexDescriptor(ktx, IndexType.VECTOR, indexName));
                PropertyIndexQuery[] queries = new PropertyIndexQuery[queryFilters.length + 2];
                queries[0] = kNearestNeighboursPredicate;
                queries[1] = entityFilterPredicate;
                for (int i = 0; i < queryFilters.length; i++) {
                    Function<TokenRead, PropertyIndexQuery> queryFilter = queryFilters[i];
                    queries[i + 2] = queryFilter == null ? null : queryFilter.apply(tokenRead);
                }
                read.relationshipIndexSeek(
                        queryContext, session, indexCursor, IndexQueryConstraints.unconstrained(), queries);

                ResultList results = new ResultList();
                while (indexCursor.next()) {
                    float score = indexCursor.score();
                    read.singleRelationship(indexCursor.reference(), relCursor);
                    while (relCursor.next()) {
                        results.add(VectorSSFQueryResult.fromCursors(
                                score, relCursor, propertyCursor, tokenRead, Set.of(EMBEDDING_NAME)));
                    }
                }
                return results;
            }
        }
    }

    @SafeVarargs
    final ResultList queryNodeIndex(Function<TokenRead, PropertyIndexQuery>... queryFilters) throws Exception {
        return queryNodeIndex(PropertyIndexQuery.matchAllEntityFilter(), queryFilters);
    }

    @SafeVarargs
    final ResultList queryNodeIndex(
            EntityFilterPredicate entityFilterPredicate, Function<TokenRead, PropertyIndexQuery>... queryFilters)
            throws Exception {
        return queryNodeIndex(
                VECTOR_INDEX_NAME,
                PropertyIndexQuery.nearestNeighbors(K_NEAREST_NEIGHBORS, EMBEDDINGS.get(0)),
                entityFilterPredicate,
                queryFilters);
    }

    @SafeVarargs
    final ResultList queryNodeIndex(int kNearestNeighbours, Function<TokenRead, PropertyIndexQuery>... queryFilters)
            throws Exception {
        return queryNodeIndex(EMBEDDINGS.get(0), kNearestNeighbours, queryFilters);
    }

    @SafeVarargs
    final ResultList queryNodeIndex(
            float[] query, int kNearestNeighbours, Function<TokenRead, PropertyIndexQuery>... queryFilters)
            throws Exception {
        return queryNodeIndex(
                        VECTOR_INDEX_NAME,
                        PropertyIndexQuery.nearestNeighbors(kNearestNeighbours, query),
                        PropertyIndexQuery.matchAllEntityFilter(),
                        queryFilters)
                .stream()
                .collect(Collectors.toCollection(ResultList::new));
    }

    @SafeVarargs
    final ResultList queryRelationshipIndex(Function<TokenRead, PropertyIndexQuery>... queryFilters) throws Exception {
        return queryRelationshipIndex(
                VECTOR_INDEX_NAME,
                PropertyIndexQuery.nearestNeighbors(K_NEAREST_NEIGHBORS, EMBEDDINGS.get(0)),
                PropertyIndexQuery.matchAllEntityFilter(),
                queryFilters);
    }

    @SafeVarargs
    final ResultList queryRelationshipIndex(
            EntityFilterPredicate entityFilterPredicate, Function<TokenRead, PropertyIndexQuery>... queryFilters)
            throws Exception {
        return queryRelationshipIndex(
                VECTOR_INDEX_NAME,
                PropertyIndexQuery.nearestNeighbors(K_NEAREST_NEIGHBORS, EMBEDDINGS.get(0)),
                entityFilterPredicate,
                queryFilters);
    }

    @SafeVarargs
    final ResultList queryRelationshipIndex(
            int kNearestNeighbours, Function<TokenRead, PropertyIndexQuery>... queryFilters) throws Exception {
        return queryRelationshipIndex(
                        VECTOR_INDEX_NAME,
                        PropertyIndexQuery.nearestNeighbors(kNearestNeighbours, EMBEDDINGS.get(0)),
                        PropertyIndexQuery.matchAllEntityFilter(),
                        queryFilters)
                .stream()
                .collect(Collectors.toCollection(ResultList::new));
    }

    protected static class TestException extends Exception {
        protected TestException(String s) {
            super(s);
        }
    }
}
