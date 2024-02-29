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
package org.neo4j.procedure.builtin;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.PropertyIndexQuery.NearestNeighborsPredicate;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SettingsAccessor.IndexConfigAccessor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarityFunctions;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.api.vector.VectorCandidate;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.util.FeatureToggles;
import org.neo4j.util.Preconditions;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

@SuppressWarnings("unused")
public class VectorIndexProcedures {
    // TODO VECTOR: is this SystemProperty thing needed, or should it simply be a static final?
    private static final long INDEX_ONLINE_QUERY_TIMEOUT_SECONDS =
            FeatureToggles.getInteger(VectorIndexProcedures.class, "INDEX_ONLINE_QUERY_TIMEOUT_SECONDS", 30);

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Transaction tx;

    @Context
    public KernelTransaction ktx;

    @Context
    public KernelVersion kernelVersion;

    @Context
    public ProcedureCallContext callContext;

    // loosely 'deprecated', should use `CREATE VECTOR INDEX...`
    @Description(
            """
            Create a named node vector index for the specified label and property with the given vector dimensionality using either the EUCLIDEAN or COSINE similarity function.
            Both similarity functions are case-insensitive.
            Use the `db.index.vector.queryNodes` procedure to query the named index.
            """)
    @Procedure(name = "db.index.vector.createNodeIndex", mode = SCHEMA)
    public void createIndex(
            @Name("indexName") String name,
            @Name("label") String label,
            @Name("propertyKey") String propertyKey,
            @Name("vectorDimension") Long vectorDimension,
            @Name("vectorSimilarityFunction") String vectorSimilarityFunction) {
        Objects.requireNonNull(name, "'indexName' must not be null");
        Objects.requireNonNull(label, "'label' must not be null");
        Objects.requireNonNull(propertyKey, "'propertyKey' must not be null");
        Objects.requireNonNull(vectorDimension, "'vectorDimension' must not be null");

        final var version = VectorIndexVersion.latestSupportedVersion(kernelVersion);
        Preconditions.checkState(
                version != VectorIndexVersion.UNKNOWN, "Vector index version `%s` is not a valid version.");
        Preconditions.checkArgument(
                1 <= vectorDimension && vectorDimension <= version.maxDimensions(),
                "'vectorDimension' must be between %d and %d inclusively".formatted(1, version.maxDimensions()));
        version.similarityFunction(
                Objects.requireNonNull(vectorSimilarityFunction, "'vectorSimilarityFunction' must not be null"));

        tx.schema()
                .indexFor(Label.label(label))
                .on(propertyKey)
                .withIndexType(IndexType.VECTOR.toPublicApi())
                .withIndexConfiguration(Map.of(
                        IndexSetting.vector_Dimensions(), vectorDimension,
                        IndexSetting.vector_Similarity_Function(), vectorSimilarityFunction))
                .withName(name)
                .create();
    }

    @Description(
            """
            Query the given node vector index.
            Returns requested number of nearest neighbors to the provided query vector,
            and their similarity score to that query vector, based on the configured similarity function for the index.
            The similarity score is a value between [0, 1]; where 0 indicates least similar, 1 most similar.
            """)
    @Procedure(name = "db.index.vector.queryNodes", mode = READ)
    public Stream<NodeNeighbor> queryNodeVectorIndex(
            @Name("indexName") String name,
            @Name("numberOfNearestNeighbours") Long numberOfNearestNeighbours,
            @Name("query") AnyValue candidateQuery)
            throws KernelException {
        final var query = validateQueryArguments(name, numberOfNearestNeighbours, candidateQuery);
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }
        return new NodeIndexQuery(tx, ktx, name).query(Math.toIntExact(numberOfNearestNeighbours), query);
    }

    @Description(
            """
            Query the given relationship vector index.
            Returns requested number of nearest neighbors to the provided query vector,
            and their similarity score to that query vector, based on the configured similarity function for the index.
            The similarity score is a value between [0, 1]; where 0 indicates least similar, 1 most similar.
            """)
    @Procedure(name = "db.index.vector.queryRelationships", mode = READ)
    public Stream<RelationshipNeighbor> queryRelationshipVectorIndex(
            @Name("indexName") String name,
            @Name("numberOfNearestNeighbours") Long numberOfNearestNeighbours,
            @Name("query") AnyValue candidateQuery)
            throws KernelException {
        final var query = validateQueryArguments(name, numberOfNearestNeighbours, candidateQuery);
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }
        return new RelationshipIndexQuery(tx, ktx, name).query(Math.toIntExact(numberOfNearestNeighbours), query);
    }

    private static VectorCandidate validateQueryArguments(
            String name, Long numberOfNearestNeighbours, AnyValue candidateQuery) {
        Objects.requireNonNull(name, "'indexName' must not be null");
        Objects.requireNonNull(numberOfNearestNeighbours, "'numberOfNearestNeighbours' must not be null");
        Preconditions.checkArgument(numberOfNearestNeighbours > 0, "'numberOfNearestNeighbours' must be positive");
        Objects.requireNonNull(candidateQuery, "'query' must not be null");
        if (candidateQuery == Values.NO_VALUE) {
            throw new IllegalArgumentException(
                    "'query' must not be NO_VALUE, which is treated as null",
                    new NullPointerException("'query' must not be null"));
        }

        final var query = VectorCandidate.maybeFrom(candidateQuery);
        if (query == null) {
            throw new IllegalArgumentException("'query' must be a non-null numerical array");
        }
        return query;
    }

    @Description("Set a vector property on a given node in a more space efficient representation than Cypher's SET.")
    @Procedure(name = "db.create.setNodeVectorProperty", mode = WRITE)
    public void setNodeVectorProperty(
            @Name("node") Node node, @Name("key") String propKey, @Name("vector") AnyValue candidateVector) {
        setVectorProperty(Objects.requireNonNull(node, "'node' must not be null"), propKey, candidateVector);
    }

    @Description("Set a vector property on a given node in a more space efficient representation than Cypher's SET.")
    @Procedure(name = "db.create.setVectorProperty", mode = WRITE, deprecatedBy = "db.create.setNodeVectorProperty")
    @Deprecated(since = "5.13.0", forRemoval = true)
    public Stream<NodeRecord> deprecatedSetVectorProperty(
            @Name("node") Node node, @Name("key") String propKey, @Name("vector") AnyValue candidateVector) {
        setNodeVectorProperty(Objects.requireNonNull(node, "'node' must not be null"), propKey, candidateVector);
        return Stream.of(new NodeRecord(node));
    }

    // specifically for the deprecated `db.create.setVectorProperty`
    public record NodeRecord(Node node) {}

    @Description(
            "Set a vector property on a given relationship in a more space efficient representation than Cypher's SET.")
    @Procedure(name = "db.create.setRelationshipVectorProperty", mode = WRITE)
    public void setRelationshipVectorProperty(
            @Name("relationship") Relationship relationship,
            @Name("key") String propKey,
            @Name("vector") AnyValue candidateQuery) {
        setVectorProperty(
                Objects.requireNonNull(relationship, "'relationship' must not be null"), propKey, candidateQuery);
    }

    public void setVectorProperty(Entity entity, String propKey, AnyValue candidateVector) {
        Objects.requireNonNull(propKey, "'key' must not be null");
        Objects.requireNonNull(candidateVector, "'vector' must not be null");
        if (candidateVector == Values.NO_VALUE) {
            throw new IllegalArgumentException(
                    "'vector' must not be NO_VALUE, which is treated as null",
                    new NullPointerException("'vector' must not be null"));
        }
        final var vector = VectorCandidate.maybeFrom(candidateVector);
        if (vector == null) {
            throw new IllegalArgumentException("'vector' must be a non-null numerical array");
        }
        // assume EUCLIDEAN as the bare minimum invariant
        entity.setProperty(propKey, VectorSimilarityFunctions.EUCLIDEAN.toValidVector(vector));
    }

    private static float[] validateAndConvertQuery(IndexDescriptor index, VectorCandidate query) {
        final var version = VectorIndexVersion.fromDescriptor(index.getIndexProvider());
        final var vectorIndexConfig = version.indexSettingValidator()
                .trustIsValidToVectorIndexConfig(new IndexConfigAccessor(index.getIndexConfig()));

        final var dimensions = vectorIndexConfig.dimensions();
        if (dimensions.isPresent() && query.dimensions() != dimensions.getAsInt()) {
            throw new IllegalArgumentException("Index query vector has %d dimensions, but indexed vectors have %d."
                    .formatted(query.dimensions(), dimensions.getAsInt()));
        }

        final var similarityFunction = vectorIndexConfig.similarityFunction();
        return similarityFunction.toValidVector(query);
    }

    private IndexDescriptor getValidIndex(String name) {
        final var index = ktx.schemaRead().indexGetForName(name);
        if (index == IndexDescriptor.NO_INDEX || index.getIndexType() != IndexType.VECTOR) {
            throw new IllegalArgumentException("There is no such vector schema index: " + name);
        }
        return index;
    }

    private static class NodeIndexQuery extends IndexQuery<NodeValueIndexCursor, NodeNeighbor> {
        private NodeIndexQuery(Transaction tx, KernelTransaction ktx, String name) {
            super(EntityType.NODE, tx, ktx, name);
        }

        @Override
        NodeValueIndexCursor cursor(
                CursorFactory cursorFactory, CursorContext cursorContext, MemoryTracker memoryTracker) {
            return cursorFactory.allocateNodeValueIndexCursor(cursorContext, memoryTracker);
        }

        @Override
        void seek(
                Read read,
                QueryContext queryContext,
                IndexReadSession session,
                NodeValueIndexCursor cursor,
                IndexQueryConstraints constraints,
                NearestNeighborsPredicate query)
                throws KernelException {
            read.nodeIndexSeek(queryContext, session, cursor, constraints, query);
        }

        @Override
        Stream<NodeNeighbor> stream(NodeValueIndexCursor cursor, int k) {
            return new NodeNeighborSpliterator(tx, cursor, k).stream();
        }
    }

    private static class RelationshipIndexQuery extends IndexQuery<RelationshipValueIndexCursor, RelationshipNeighbor> {
        private RelationshipIndexQuery(Transaction tx, KernelTransaction ktx, String name) {
            super(EntityType.RELATIONSHIP, tx, ktx, name);
        }

        @Override
        RelationshipValueIndexCursor cursor(
                CursorFactory cursorFactory, CursorContext cursorContext, MemoryTracker memoryTracker) {
            return cursorFactory.allocateRelationshipValueIndexCursor(cursorContext, memoryTracker);
        }

        @Override
        void seek(
                Read read,
                QueryContext queryContext,
                IndexReadSession session,
                RelationshipValueIndexCursor cursor,
                IndexQueryConstraints constraints,
                NearestNeighborsPredicate query)
                throws KernelException {
            read.relationshipIndexSeek(queryContext, session, cursor, constraints, query);
        }

        @Override
        Stream<RelationshipNeighbor> stream(RelationshipValueIndexCursor cursor, int k) {
            return new RelationshipNeighborSpliterator(tx, cursor, k).stream();
        }
    }

    private abstract static class IndexQuery<CURSOR extends ValueIndexCursor, NEIGHBOR extends Neighbor<?, NEIGHBOR>> {
        protected final Transaction tx;
        private final KernelTransaction ktx;
        private final IndexDescriptor index;

        private IndexQuery(EntityType entityType, Transaction tx, KernelTransaction ktx, String name) {
            this.tx = tx;
            this.ktx = ktx;

            final var index = ktx.schemaRead().indexGetForName(name);
            if (index == IndexDescriptor.NO_INDEX || index.getIndexType() != IndexType.VECTOR) {
                throw new IllegalArgumentException("There is no such vector schema index: " + name);
            }

            final var entityTypeFromIndex = index.schema().entityType();
            if (entityTypeFromIndex != entityType) {
                throw new IllegalArgumentException(
                        "The '%s' index (%s) is an index on %s, so it cannot be queried for nodes."
                                .formatted(index.getName(), index, entityTypeFromIndex));
            }

            this.index = index;
            awaitIndexOnline();
        }

        abstract CURSOR cursor(CursorFactory cursorFactory, CursorContext cursorContext, MemoryTracker memoryTracker);

        abstract void seek(
                Read read,
                QueryContext queryContext,
                IndexReadSession index,
                CURSOR cursor,
                IndexQueryConstraints constraints,
                NearestNeighborsPredicate query)
                throws KernelException;

        abstract Stream<NEIGHBOR> stream(CURSOR cursor, int k);

        Stream<NEIGHBOR> query(int k, VectorCandidate query) throws KernelException {
            final var validatedQuery = validateAndConvertQuery(index, query);
            final var cursor = cursor(ktx.cursors(), ktx.cursorContext(), ktx.memoryTracker());
            seek(
                    ktx.dataRead(),
                    ktx.queryContext(),
                    ktx.dataRead().indexReadSession(index),
                    cursor,
                    IndexQueryConstraints.unconstrained(),
                    PropertyIndexQuery.nearestNeighbors(k, validatedQuery));
            return stream(cursor, k);
        }

        private void awaitIndexOnline() {
            // We do the isAdded check on the transaction state first, because indexGetState will grab a schema
            // read-lock which can deadlock on the write-lock
            // held by the index populator. Also, if the index was created in this transaction, then we will never see
            // it come online in this transaction anyway.
            // Indexes don't come online until the transaction that creates them has committed.
            final var txStateHolder = (TxStateHolder) ktx;
            if (!txStateHolder.hasTxStateWithChanges()
                    || !txStateHolder
                            .txState()
                            .indexDiffSetsBySchema(index.schema())
                            .isAdded(index)) {
                // If the index was not created in this transaction, then wait for it to come online before querying.
                tx.schema().awaitIndexOnline(index.getName(), INDEX_ONLINE_QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            // If the index was created in this transaction, then we skip this check entirely.
            // We will get an exception later, when we try to get an IndexReader, so this is fine.
        }
    }

    /**
     * @param node a node within the query point's neighborhood
     * @param score similarity in [0, 1]; 0 indicates furthest, 1 closest.
     */
    public record NodeNeighbor(Node node, double score) implements Neighbor<Node, NodeNeighbor> {
        @Override
        public Node entity() {
            return node;
        }

        public static NodeNeighbor forExistingEntityOrNull(Transaction tx, long nodeId, double score) {
            try {
                return new NodeNeighbor(tx.getNodeById(nodeId), score);
            } catch (NotFoundException ignore) {
                // This node was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }
    }

    /**
     * @param relationship a relationship within the query point's neighborhood
     * @param score similarity in [0, 1]; 0 indicates furthest, 1 closest.
     */
    public record RelationshipNeighbor(Relationship relationship, double score)
            implements Neighbor<Relationship, RelationshipNeighbor> {
        @Override
        public Relationship entity() {
            return relationship;
        }

        public static RelationshipNeighbor forExistingEntityOrNull(Transaction tx, long relId, double score) {
            try {
                return new RelationshipNeighbor(tx.getRelationshipById(relId), score);
            } catch (NotFoundException ignore) {
                // This relationship was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }
    }

    public interface Neighbor<ENTITY extends Entity, NEIGHBOR extends Neighbor<ENTITY, NEIGHBOR>>
            extends Comparable<NEIGHBOR> {
        ENTITY entity();

        /**
         * @return similarity in [0, 1]; 0 indicates furthest, 1 closest
         */
        double score();

        @Override
        default int compareTo(NEIGHBOR o) {
            return -Double.compare(this.score(), o.score()); // order switched, 0 furthest, 1 closed
        }
    }

    private record NodeNeighborSpliterator(Transaction tx, NodeValueIndexCursor cursor, int k)
            implements NeighborSpliterator<NodeNeighbor> {
        @Override
        public NodeNeighbor neighbor() {
            return NodeNeighbor.forExistingEntityOrNull(
                    tx, cursor.nodeReference(), MathUtil.clamp(cursor.score(), 0.0, 1.0));
        }
    }

    private record RelationshipNeighborSpliterator(Transaction tx, RelationshipValueIndexCursor cursor, int k)
            implements NeighborSpliterator<RelationshipNeighbor> {
        @Override
        public RelationshipNeighbor neighbor() {
            return RelationshipNeighbor.forExistingEntityOrNull(
                    tx, cursor.relationshipReference(), MathUtil.clamp(cursor.score(), 0.0, 1.0));
        }
    }

    private interface NeighborSpliterator<NEIGHBOR extends Neighbor<?, NEIGHBOR>> extends Spliterator<NEIGHBOR> {
        int k();

        Cursor cursor();

        NEIGHBOR neighbor();

        @Override
        default boolean tryAdvance(Consumer<? super NEIGHBOR> action) {
            final var cursor = cursor();
            while (cursor.next()) {
                final var neighbor = neighbor();
                if (neighbor != null) {
                    action.accept(neighbor);
                    return true;
                }
            }
            cursor.close();
            return false;
        }

        @Override
        default long estimateSize() {
            return k();
        }

        @Override
        default Spliterator<NEIGHBOR> trySplit() {
            return null;
        }

        @Override
        default int characteristics() {
            return Spliterator.ORDERED
                    | Spliterator.SORTED
                    | Spliterator.DISTINCT
                    | Spliterator.NONNULL
                    | Spliterator.IMMUTABLE;
        }

        @Override
        default Comparator<? super NEIGHBOR> getComparator() {
            return null;
        }

        default Stream<NEIGHBOR> stream() {
            final var stream = StreamSupport.stream(this, false);
            return stream.onClose(cursor()::close);
        }
    }
}
