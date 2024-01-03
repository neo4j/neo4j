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

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.kernel.api.impl.schema.vector.VectorUtils.vectorDimensionsFrom;
import static org.neo4j.kernel.api.impl.schema.vector.VectorUtils.vectorSimilarityFunctionFrom;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarityFunction;
import org.neo4j.kernel.api.impl.schema.vector.VectorUtils;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.util.FeatureToggles;
import org.neo4j.util.Preconditions;

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
    public ProcedureCallContext callContext;

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
        Preconditions.checkArgument(
                1 <= vectorDimension && vectorDimension <= VectorUtils.MAX_DIMENSIONS,
                "'vectorDimension' must be between %d and %d inclusively".formatted(1, VectorUtils.MAX_DIMENSIONS));
        VectorSimilarityFunction.fromName(
                Objects.requireNonNull(vectorSimilarityFunction, "'vectorSimilarityFunction' must not be null"));

        final var indexCreator = tx.schema()
                .indexFor(Label.label(label))
                .on(propertyKey)
                .withIndexType(IndexType.VECTOR.toPublicApi())
                .withIndexConfiguration(Map.of(
                        IndexSetting.vector_Dimensions(), vectorDimension,
                        IndexSetting.vector_Similarity_Function(), vectorSimilarityFunction))
                .withName(name);
        indexCreator.create();
    }

    @Description(
            """
            Query the given vector index.
            Returns requested number of nearest neighbors to the provided query vector,
            and their similarity score to that query vector, based on the configured similarity function for the index.
            The similarity score is a value between [0, 1]; where 0 indicates least similar, 1 most similar.
            """)
    @Procedure(name = "db.index.vector.queryNodes", mode = READ)
    public Stream<Neighbor> queryVectorIndex(
            @Name("indexName") String name,
            @Name("numberOfNearestNeighbours") Long numberOfNearestNeighbours,
            @Name("query") List<Double> query)
            throws KernelException {
        Objects.requireNonNull(name, "'indexName' must not be null");
        Objects.requireNonNull(numberOfNearestNeighbours, "'numberOfNearestNeighbours' must not be null");
        Preconditions.checkArgument(numberOfNearestNeighbours > 0, "'numberOfNearestNeighbours' must be positive");
        Objects.requireNonNull(query, "'query' must not be null");

        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        final var index = getValidIndex(name);
        final var validatedQuery = validateAndConvertQuery(index, query);

        awaitOnline(index);

        final var entityType = index.schema().entityType();
        if (entityType != NODE) {
            throw new IllegalArgumentException(
                    "The '%s' index (%s) is an index on %s, so it cannot be queried for nodes."
                            .formatted(name, index, entityType));
        }

        final var cursor = ktx.cursors().allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker());
        final var session = ktx.dataRead().indexReadSession(index);
        final var constraints = IndexQueryConstraints.unconstrained();

        final var k = Math.toIntExact(numberOfNearestNeighbours);
        ktx.dataRead()
                .nodeIndexSeek(
                        ktx.queryContext(),
                        session,
                        cursor,
                        constraints,
                        PropertyIndexQuery.nearestNeighbors(k, validatedQuery));

        return new NeighborSpliterator(tx, cursor, k).stream();
    }

    @Description("Set a vector property on a given node in a more space efficient representation than Cypher's SET.")
    @Procedure(name = "db.create.setNodeVectorProperty", mode = WRITE)
    public void setNodeVectorProperty(
            @Name("node") Node node, @Name("key") String propKey, @Name("vector") List<Double> vector) {
        setVectorProperty(
                Objects.requireNonNull(node, "'node' must not be null"),
                Objects.requireNonNull(propKey, "'key' must not be null"),
                Objects.requireNonNull(vector, "'vector' must not be null"));
    }

    @Description("Set a vector property on a given node in a more space efficient representation than Cypher's SET.")
    @Procedure(name = "db.create.setVectorProperty", mode = WRITE, deprecatedBy = "db.create.setNodeVectorProperty")
    @Deprecated(since = "5.13.0", forRemoval = true)
    public Stream<NodeRecord> deprecatedSetVectorProperty(
            @Name("node") Node node, @Name("key") String propKey, @Name("vector") List<Double> vector) {
        setNodeVectorProperty(node, propKey, vector);
        return Stream.of(new NodeRecord(node));
    }

    public record NodeRecord(Node node) {}

    private void setVectorProperty(Entity entity, String propKey, List<Double> vector) {
        // assume EUCLIDEAN as the bare minimum invariant
        entity.setProperty(propKey, VectorSimilarityFunction.EUCLIDEAN.toValidVector(vector));
    }

    private float[] validateAndConvertQuery(IndexDescriptor index, List<Double> query) {
        final var config = index.getIndexConfig();
        final var dimensions = vectorDimensionsFrom(config);
        if (dimensions != query.size()) {
            throw new IllegalArgumentException("Index query vector has %d dimensions, but indexed vectors have %d."
                    .formatted(query.size(), dimensions));
        }

        final var similarityFunction = vectorSimilarityFunctionFrom(config);
        return similarityFunction.toValidVector(query);
    }

    private IndexDescriptor getValidIndex(String name) {
        final var index = ktx.schemaRead().indexGetForName(name);
        if (index == IndexDescriptor.NO_INDEX || index.getIndexType() != IndexType.VECTOR) {
            throw new IllegalArgumentException("There is no such vector schema index: " + name);
        }
        return index;
    }

    private void awaitOnline(IndexDescriptor index) {
        // We do the isAdded check on the transaction state first, because indexGetState will grab a schema read-lock,
        // which can deadlock on the write-lock
        // held by the index populator. Also, if the index was created in this transaction, then we will never see it
        // come online in this transaction anyway.
        // Indexes don't come online until the transaction that creates them has committed.
        final var ktx = (TxStateHolder) this.ktx;
        if (!ktx.hasTxStateWithChanges()
                || !ktx.txState().indexDiffSetsBySchema(index.schema()).isAdded(index)) {
            // If the index was not created in this transaction, then wait for it to come online before querying.
            tx.schema().awaitIndexOnline(index.getName(), INDEX_ONLINE_QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        // If the index was created in this transaction, then we skip this check entirely.
        // We will get an exception later, when we try to get an IndexReader, so this is fine.
    }

    /**
     * @param node a node within the query point's neighborhood
     * @param score similarity in [0, 1]; 0 indicates furthest, 1 closest.
     */
    public record Neighbor(Node node, double score) implements Comparable<Neighbor> {
        @Override
        public int compareTo(Neighbor o) {
            final int result = -Double.compare(this.score, o.score); // order switched, 0 furthest, 1 closed
            if (result != 0) {
                return result;
            }
            return Long.compare(node.getId(), o.node.getId());
        }

        public static Neighbor forExistingEntityOrNull(Transaction tx, long nodeId, float score) {
            try {
                return new Neighbor(tx.getNodeById(nodeId), score);
            } catch (NotFoundException ignore) {
                // This node was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }
    }

    private record NeighborSpliterator(Transaction tx, NodeValueIndexCursor cursor, int k)
            implements Spliterator<Neighbor> {
        @Override
        public boolean tryAdvance(Consumer<? super Neighbor> action) {
            while (cursor.next()) {
                final var neighbor = Neighbor.forExistingEntityOrNull(tx, cursor.nodeReference(), cursor.score());
                if (neighbor != null) {
                    action.accept(neighbor);
                    return true;
                }
            }
            cursor.close();
            return false;
        }

        @Override
        public Spliterator<Neighbor> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return k;
        }

        @Override
        public int characteristics() {
            return Spliterator.ORDERED
                    | Spliterator.SORTED
                    | Spliterator.DISTINCT
                    | Spliterator.NONNULL
                    | Spliterator.IMMUTABLE;
        }

        @Override
        public Comparator<? super Neighbor> getComparator() {
            return null;
        }

        Stream<Neighbor> stream() {
            final var stream = StreamSupport.stream(this, false);
            return stream.onClose(cursor::close);
        }
    }
}
