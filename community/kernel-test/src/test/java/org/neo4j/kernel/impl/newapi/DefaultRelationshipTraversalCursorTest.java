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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;

class DefaultRelationshipTraversalCursorTest {
    private static final long node = 42;
    private static final int type = 9999;
    private static final int type2 = 9998;
    private static final long relationship = 100;
    private final DefaultPooledCursors pool = mock(DefaultPooledCursors.class);
    private final InternalCursorFactory internalCursors = MockedInternalCursors.mockedInternalCursors();

    // Regular traversal of a sparse chain

    @Test
    void regularTraversal() {
        // given
        StorageRelationshipTraversalCursor storeCursor = storeCursor(100, 102, 104);
        DefaultRelationshipTraversalCursor cursor =
                new DefaultRelationshipTraversalCursor(pool::accept, storeCursor, internalCursors, false);
        Read read = emptyTxState();

        // when
        cursor.init(node, relationship, ALL_RELATIONSHIPS, read);

        // then
        assertRelationships(cursor, 100, 102, 104);
    }

    @Test
    void regularTraversalWithTxState() {
        // given
        StorageRelationshipTraversalCursor storeCursor = storeCursor(100, 102, 104);
        DefaultRelationshipTraversalCursor cursor =
                new DefaultRelationshipTraversalCursor(pool::accept, storeCursor, internalCursors, false);
        Read read = txState(3, 4);

        // when
        cursor.init(node, relationship, ALL_RELATIONSHIPS, read);

        // then
        assertRelationships(cursor, 3, 4, 100, 102, 104);
    }

    // Sparse traversal but with tx-state filtering

    @Test
    void traversalWithTxStateFiltering() {
        // given
        StorageRelationshipTraversalCursor storeCursor = storeCursor(
                rel(100, node, 50, type), // <- the filter template
                rel(102, node, 51, type),
                rel(104, node, 52, type));

        DefaultRelationshipTraversalCursor cursor =
                new DefaultRelationshipTraversalCursor(pool::accept, storeCursor, internalCursors, false);
        Read read = txState(
                rel(3, node, 50, type),
                rel(4, 50, node, type),
                rel(5, node, 50, type2),
                rel(6, node, node, type),
                rel(7, node, 52, type));

        // when
        cursor.init(
                node,
                relationship,
                // relationships of a specific type/direction
                selection(type, Direction.OUTGOING),
                read);

        // then
        assertRelationships(cursor, 3, 7, 6, 100, 102, 104);
    }

    // Empty store, but filter tx-state

    @Test
    void emptyStoreOutgoingOfType() {
        // given
        StorageRelationshipTraversalCursor storeCursor = emptyStoreCursor();

        DefaultRelationshipTraversalCursor cursor =
                new DefaultRelationshipTraversalCursor(pool::accept, storeCursor, internalCursors, false);
        Read read = txState(
                rel(3, node, 50, type),
                rel(4, 50, node, type),
                rel(5, node, 50, type2),
                rel(6, node, node, type),
                rel(7, node, 52, type));

        // when
        cursor.init(node, relationship, selection(type, Direction.OUTGOING), read);

        // then
        assertRelationships(cursor, 3, 7, 6);
    }

    @Test
    void emptyStoreIncomingOfType() {
        // given
        StorageRelationshipTraversalCursor storeCursor = emptyStoreCursor();

        DefaultRelationshipTraversalCursor cursor =
                new DefaultRelationshipTraversalCursor(pool::accept, storeCursor, internalCursors, false);
        Read read = txState(
                rel(3, node, 50, type),
                rel(4, 50, node, type),
                rel(5, 50, node, type2),
                rel(6, node, node, type),
                rel(7, 56, node, type),
                rel(8, node, 52, type));

        // when
        cursor.init(node, relationship, selection(type, Direction.INCOMING), read);

        // then
        assertRelationships(cursor, 4, 7, 6);
    }

    @Test
    void emptyStoreAllOfType() {
        // given
        StorageRelationshipTraversalCursor storeCursor = emptyStoreCursor();

        DefaultRelationshipTraversalCursor cursor =
                new DefaultRelationshipTraversalCursor(pool::accept, storeCursor, internalCursors, false);
        Read read = txState(
                rel(3, node, 50, type),
                rel(2, node, node, type),
                rel(5, 50, node, type2),
                rel(6, node, node, type),
                rel(7, 56, node, type),
                rel(8, node, 52, type));

        // when
        cursor.init(node, relationship, selection(type, Direction.BOTH), read);

        // then
        assertRelationships(cursor, 3, 8, 7, 2, 6);
    }

    @Test
    void readOfRelationshipFromTxState() {
        // given
        final var startId = 56;
        final var relId = 7;
        final var endId = 42;

        final var storeCursor = emptyStoreCursor();
        final var cursor = new DefaultRelationshipTraversalCursor(pool::accept, storeCursor, internalCursors, false);
        final var read = txState(
                rel(3, node, 50, type),
                rel(2, node, node, type),
                rel(5, 50, node, type2),
                rel(6, node, node, type),
                rel(relId, startId, endId, type2),
                rel(8, node, 52, type));

        // when
        cursor.init(relId, read);

        // then
        assertThat(cursor.next()).isTrue();
        assertThat(cursor.sourceNodeReference()).isEqualTo(startId);
        assertThat(cursor.targetNodeReference()).isEqualTo(endId);
        assertThat(cursor.relationshipReference()).isEqualTo(relId);
        assertThat(cursor.type()).isEqualTo(type2);
        assertThat(cursor.next()).isFalse();
    }

    // HELPERS

    private static Read emptyTxState() {
        KernelTransactionImplementation ktx = mock(KernelTransactionImplementation.class);
        when(ktx.securityContext()).thenReturn(SecurityContext.AUTH_DISABLED);
        return new TestRead(ktx);
    }

    private static Read txState(long... ids) {
        return txState(
                LongStream.of(ids).mapToObj(id -> rel(id, node, node, type)).toArray(Rel[]::new));
    }

    private static Read txState(Rel... rels) {
        KernelTransactionImplementation ktx = mock(KernelTransactionImplementation.class);
        when(ktx.securityContext()).thenReturn(SecurityContext.AUTH_DISABLED);
        Read read = new TestRead(ktx);
        if (rels.length > 0) {
            TxState txState = new TxState();
            for (Rel rel : rels) {
                txState.relationshipDoCreate(rel.relId, rel.type, rel.sourceId, rel.targetId);
            }
            when(ktx.hasTxStateWithChanges()).thenReturn(true);
            when(ktx.txState()).thenReturn(txState);
        }
        return read;
    }

    private static void assertRelationships(DefaultRelationshipTraversalCursor cursor, long... expected) {
        for (long expectedId : expected) {
            assertTrue(cursor.next(), "Expected relationship " + expectedId + " but got none");
            assertEquals(
                    expectedId,
                    cursor.relationshipReference(),
                    "Expected relationship " + expectedId + " got " + cursor.relationshipReference());
        }
        assertFalse(cursor.next(), "Expected no more relationships, but got " + cursor.relationshipReference());
    }

    private static Rel rel(long relId, long startId, long endId, int type) {
        return new Rel(relId, startId, endId, type);
    }

    private static final Rel NO_REL = rel(-1L, -1L, -1L, -1);

    private record Rel(long relId, long sourceId, long targetId, int type) {
        RelationshipDirection direction(long nodeReference) {
            if (sourceId == targetId) {
                return RelationshipDirection.LOOP;
            }
            return nodeReference == sourceId ? RelationshipDirection.OUTGOING : RelationshipDirection.INCOMING;
        }
    }

    private static StorageRelationshipTraversalCursor emptyStoreCursor() {
        return storeCursor(new Rel[0]);
    }

    private static StorageRelationshipTraversalCursor storeCursor(long... ids) {
        return storeCursor(
                LongStream.of(ids).mapToObj(id -> rel(id, -1L, -1L, -1)).toArray(Rel[]::new));
    }

    private static StorageRelationshipTraversalCursor storeCursor(Rel... rels) {
        return new StorageRelationshipTraversalCursor() {
            private long nodeReference;
            private RelationshipSelection selection;
            private int i = -1;
            private Rel rel = NO_REL;

            @Override
            public long neighbourNodeReference() {
                return rel.sourceId == node ? rel.targetId : rel.sourceId;
            }

            @Override
            public long originNodeReference() {
                return node;
            }

            @Override
            public void init(long nodeReference, long reference, RelationshipSelection selection) {
                this.nodeReference = nodeReference;
                this.selection = selection;
            }

            @Override
            public int type() {
                return rel.type;
            }

            @Override
            public long sourceNodeReference() {
                return rel.sourceId;
            }

            @Override
            public long targetNodeReference() {
                return rel.targetId;
            }

            @Override
            public boolean hasProperties() {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public Reference propertiesReference() {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void properties(StoragePropertyCursor propertyCursor, PropertySelection selection) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public long entityReference() {
                return rel.relId;
            }

            @Override
            public boolean next() {
                while (i + 1 < rels.length) {
                    i++;
                    if (i < 0 || i >= rels.length) {
                        rel = NO_REL;
                        return false;
                    } else {
                        rel = rels[i];
                        if (selection.test(rel.type, rel.direction(nodeReference))) {
                            return true;
                        }
                    }
                }
                return false;
            }

            @Override
            public void reset() {}

            @Override
            public void setForceLoad() {}

            @Override
            public void close() {}
        };
    }

    private static class TestRead extends Read {

        private final KernelTransactionImplementation ktx;

        TestRead(KernelTransactionImplementation ktx) {
            super(
                    mock(StorageReader.class),
                    ktx.tokenRead(),
                    mock(DefaultPooledCursors.class),
                    ktx.storeCursors(),
                    mock(StorageLocks.class),
                    ktx.lockTracer());
            this.ktx = ktx;
        }

        @Override
        public ValueIndexReader newValueIndexReader(IndexDescriptor index) {
            return null;
        }

        @Override
        void performCheckBeforeOperation() {
            ktx.assertOpen();
        }

        @Override
        AccessMode getAccessMode() {
            return ktx.securityContext().mode();
        }

        @Override
        LockManager.Client getLockClient() {
            return ktx.lockClient();
        }

        @Override
        public UserFunctionHandle functionGet(QualifiedName name) {
            return null;
        }

        @Override
        public Stream<UserFunctionSignature> functionGetAll() {
            return null;
        }

        @Override
        public UserFunctionHandle aggregationFunctionGet(QualifiedName name) {
            return null;
        }

        @Override
        public Stream<UserFunctionSignature> aggregationFunctionGetAll() {
            return null;
        }

        @Override
        public ProcedureHandle procedureGet(QualifiedName name) {
            return null;
        }

        @Override
        public Set<ProcedureSignature> proceduresGetAll() {
            return null;
        }

        @Override
        public RawIterator<AnyValue[], ProcedureException> procedureCallRead(
                int id, AnyValue[] arguments, ProcedureCallContext context) {
            return null;
        }

        @Override
        public RawIterator<AnyValue[], ProcedureException> procedureCallWrite(
                int id, AnyValue[] arguments, ProcedureCallContext context) {
            return null;
        }

        @Override
        public RawIterator<AnyValue[], ProcedureException> procedureCallSchema(
                int id, AnyValue[] arguments, ProcedureCallContext context) {
            return null;
        }

        @Override
        public RawIterator<AnyValue[], ProcedureException> procedureCallDbms(
                int id, AnyValue[] arguments, ProcedureCallContext context) {
            return null;
        }

        @Override
        public AnyValue functionCall(int id, AnyValue[] arguments, ProcedureCallContext context) {
            return null;
        }

        @Override
        public AnyValue builtInFunctionCall(int id, AnyValue[] arguments, ProcedureCallContext context) {
            return null;
        }

        @Override
        public UserAggregationReducer aggregationFunction(int id, ProcedureCallContext context) {
            return null;
        }

        @Override
        public UserAggregationReducer builtInAggregationFunction(int id, ProcedureCallContext context) {
            return null;
        }

        @Override
        public long signatureVersion() {
            return 0;
        }

        @Override
        public CursorContext cursorContext() {
            return null;
        }

        @Override
        public MemoryTracker memoryTracker() {
            return null;
        }

        @Override
        public IndexMonitor monitor() {
            return IndexMonitor.NO_MONITOR;
        }

        @Override
        public IndexReadSession indexReadSession(IndexDescriptor index) {
            return null;
        }

        @Override
        public TokenReadSession tokenReadSession(IndexDescriptor index) throws IndexNotFoundKernelException {
            return null;
        }

        @Override
        public boolean nodeExists(long reference) {
            return false;
        }

        @Override
        public long countsForNode(int labelId) {
            return 0;
        }

        @Override
        public long estimateCountsForNode(int labelId) {
            return 0;
        }

        @Override
        public List<Integer> mostCommonLabelGivenRelationshipType(int type) {
            return Collections.emptyList();
        }

        @Override
        public long countsForRelationship(int startLabelId, int typeId, int endLabelId) {
            return 0;
        }

        @Override
        public long estimateCountsForRelationships(int startLabelId, int typeId, int endLabelId) {
            return 0;
        }

        @Override
        public long nodesGetCount() {
            return 0;
        }

        @Override
        public long relationshipsGetCount() {
            return 0;
        }

        @Override
        public boolean relationshipExists(long reference) {
            return false;
        }

        @Override
        public boolean nodeDeletedInTransaction(long node) {
            return false;
        }

        @Override
        public boolean relationshipDeletedInTransaction(long relationship) {
            return false;
        }

        @Override
        public Value nodePropertyChangeInBatchOrNull(long node, int propertyKeyId) {
            return null;
        }

        @Override
        public Value relationshipPropertyChangeInBatchOrNull(long relationship, int propertyKeyId) {
            return null;
        }

        @Override
        public boolean transactionStateHasChanges() {
            return false;
        }

        @Override
        public Iterator<IndexDescriptor> indexForSchemaNonTransactional(SchemaDescriptor schema) {
            return null;
        }

        @Override
        public IndexDescriptor indexForSchemaAndIndexTypeNonTransactional(
                SchemaDescriptor schema, IndexType indexType) {
            return null;
        }

        @Override
        public Iterator<IndexDescriptor> indexForSchemaNonLocking(SchemaDescriptor schema) {
            return null;
        }

        @Override
        public Iterator<IndexDescriptor> getLabelIndexesNonLocking(int labelId) {
            return null;
        }

        @Override
        public Iterator<IndexDescriptor> getRelTypeIndexesNonLocking(int labelId) {
            return null;
        }

        @Override
        public Iterator<IndexDescriptor> indexesGetAllNonLocking() {
            return null;
        }

        @Override
        public double indexUniqueValuesSelectivity(IndexDescriptor index) {
            return 0;
        }

        @Override
        public long indexSize(IndexDescriptor index) {
            return 0;
        }

        @Override
        public IndexSample indexSample(IndexDescriptor index) {
            return null;
        }

        @Override
        public Iterator<ConstraintDescriptor> constraintsGetForSchema(SchemaDescriptor descriptor) {
            return null;
        }

        @Override
        public Iterator<ConstraintDescriptor> constraintsGetForSchemaNonLocking(SchemaDescriptor descriptor) {
            return null;
        }

        @Override
        public boolean constraintExists(ConstraintDescriptor descriptor) {
            return false;
        }

        @Override
        public SchemaReadCore snapshot() {
            return null;
        }

        @Override
        public Long indexGetOwningUniquenessConstraintId(IndexDescriptor index) {
            return null;
        }

        @Override
        public Long indexGetOwningUniquenessConstraintIdNonLocking(IndexDescriptor index) {
            return null;
        }

        @Override
        public <K, V> V schemaStateGetOrCreate(K key, Function<K, V> creator) {
            return null;
        }

        @Override
        public void schemaStateFlush() {}

        @Override
        public IndexDescriptor indexGetForName(String name) {
            return null;
        }

        @Override
        public ConstraintDescriptor constraintGetForName(String name) {
            return null;
        }

        @Override
        public Iterator<IndexDescriptor> index(SchemaDescriptor schema) {
            return null;
        }

        @Override
        public IndexDescriptor index(SchemaDescriptor schema, IndexType type) {
            return null;
        }

        @Override
        public Iterator<IndexDescriptor> indexesGetForLabel(int labelId) {
            return null;
        }

        @Override
        public Iterator<IndexDescriptor> indexesGetForRelationshipType(int relationshipType) {
            return null;
        }

        @Override
        public Iterator<IndexDescriptor> indexesGetAll() {
            return null;
        }

        @Override
        public InternalIndexState indexGetState(IndexDescriptor index) {
            return null;
        }

        @Override
        public InternalIndexState indexGetStateNonLocking(IndexDescriptor index) {
            return null;
        }

        @Override
        public PopulationProgress indexGetPopulationProgress(IndexDescriptor index) {
            return null;
        }

        @Override
        public String indexGetFailure(IndexDescriptor index) {
            return null;
        }

        @Override
        public Iterator<ConstraintDescriptor> constraintsGetForLabel(int labelId) {
            return null;
        }

        @Override
        public Iterator<ConstraintDescriptor> constraintsGetForLabelNonLocking(int labelId) {
            return null;
        }

        @Override
        public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType(int typeId) {
            return null;
        }

        @Override
        public Iterator<ConstraintDescriptor> constraintsGetForRelationshipTypeNonLocking(int typeId) {
            return null;
        }

        @Override
        public Iterator<ConstraintDescriptor> constraintsGetAll() {
            return null;
        }

        @Override
        public Iterator<ConstraintDescriptor> constraintsGetAllNonLocking() {
            return null;
        }

        @Override
        public TransactionState txState() {
            return ktx.txState();
        }

        @Override
        public boolean hasTxStateWithChanges() {
            return ktx.hasTxStateWithChanges();
        }

        @Override
        public IndexUsageStats indexUsageStats(IndexDescriptor index) {
            return null;
        }
    }
}
