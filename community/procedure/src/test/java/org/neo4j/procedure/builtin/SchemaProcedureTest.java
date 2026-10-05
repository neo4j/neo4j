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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.procedure.impl.memory.ProcedureMemoryProvider;
import org.neo4j.procedure.memory.ProcedureMemory;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenConstants;

class SchemaProcedureTest {
    private final InternalTransaction internalTransaction = mock(InternalTransaction.class);
    private final KernelTransaction tx = mock(KernelTransaction.class);
    private final Read dataRead = mock(Read.class);
    private final TokenRead tokenRead = mock(TokenRead.class);
    private final CursorFactory cursors = mock(CursorFactory.class);
    private final SchemaRead schemaRead = mock(SchemaRead.class);
    private final ProcedureMemory procedureMemory;

    SchemaProcedureTest() throws ProcedureException {
        ProcedureCallContext callContext = mock(ProcedureCallContext.class);
        when(callContext.memoryTracker()).thenReturn(EmptyMemoryTracker.INSTANCE);
        var context = mock(Context.class);
        when(context.procedureCallContext()).thenReturn(callContext);
        procedureMemory = new ProcedureMemoryProvider().apply(context);
    }

    @BeforeEach
    void setUp() throws IndexNotFoundKernelException {
        when(internalTransaction.kernelTransaction()).thenReturn(tx);
        when(tx.dataRead()).thenReturn(dataRead);
        when(tx.tokenRead()).thenReturn(tokenRead);
        when(tx.cursors()).thenReturn(cursors);
        when(tx.cursorContext()).thenReturn(CursorContext.NULL_CONTEXT);
        when(tx.memoryTracker()).thenReturn(EmptyMemoryTracker.INSTANCE);

        var securityContext = mock(SecurityContext.class);
        when(tx.securityContext()).thenReturn(securityContext);
        var mode = mock(AccessMode.class);
        when(securityContext.mode()).thenReturn(mode);
        when(mode.allowsTraverseNode(any(int[].class))).thenReturn(true);
        when(mode.allowsTraverseRelType(anyInt())).thenReturn(true);

        var schemaReadCore = mock(SchemaReadCore.class);
        when(tx.schemaRead()).thenReturn(schemaRead);
        when(schemaRead.snapshot()).thenReturn(schemaReadCore);
        when(schemaRead.indexesGetForLabel(anyInt())).thenReturn(Collections.emptyIterator());
        when(schemaRead.constraintsGetForLabel(anyInt())).thenReturn(Collections.emptyIterator());
        when(schemaReadCore.indexesGetForLabel(anyInt())).thenReturn(Collections.emptyIterator());
        when(schemaReadCore.constraintsGetForLabel(anyInt())).thenReturn(Collections.emptyIterator());
        when(schemaReadCore.indexesGetForRelationshipType(anyInt())).thenReturn(Collections.emptyIterator());
        when(schemaReadCore.constraintsGetForRelationshipType(anyInt())).thenReturn(Collections.emptyIterator());

        var tokenIndex = mock(IndexDescriptor.class);
        when(schemaRead.index(any()))
                .thenAnswer(invocation -> Collections.singletonList(tokenIndex).iterator());
        when(dataRead.tokenReadSession(any())).thenReturn(mock(TokenReadSession.class));

        when(tokenRead.labelsGetAllTokens()).thenReturn(Collections.emptyIterator());
        when(tokenRead.relationshipTypesGetAllTokens()).thenReturn(Collections.emptyIterator());
    }

    private void givenLabels(NamedToken... tokens) throws LabelNotFoundKernelException {
        when(tokenRead.labelsGetAllTokens()).thenReturn(Arrays.stream(tokens).iterator());
        for (NamedToken token : tokens) {
            when(tokenRead.nodeLabel(token.name())).thenReturn(token.id());
            when(tokenRead.nodeLabelName(token.id())).thenReturn(token.name());
            when(dataRead.countsForNode(token.id())).thenReturn(1L);
        }
    }

    private void givenRelationshipType(String name, int id) throws KernelException {
        when(tokenRead.relationshipTypesGetAllTokens())
                .thenReturn(Collections.singletonList(new NamedToken(name, id)).iterator());
        when(tokenRead.relationshipType(name)).thenReturn(id);
        when(tokenRead.relationshipTypeName(id)).thenReturn(name);
        when(dataRead.countsForRelationship(anyInt(), anyInt(), anyInt())).thenReturn(1L);
    }

    private void givenNodeCount(int labelId, long count) {
        when(dataRead.estimateCountsForNode(labelId)).thenReturn(count);
    }

    private void givenRelationshipCount(int startLabel, int type, int endLabel, long count) {
        when(dataRead.estimateCountsForRelationships(startLabel, type, endLabel))
                .thenReturn(count);
    }

    @Test
    void shouldReturnEmptyGraph() {
        var result = new SchemaProcedure(internalTransaction, procedureMemory, procedureMemory.newTracker())
                .buildSchemaGraph(false);

        assertThat(result.nodes()).isEmpty();
        assertThat(result.relationships()).isEmpty();
    }

    @Test
    void shouldReturnOrphanedNodeForLabelWithoutRelationships() throws KernelException {
        givenLabels(new NamedToken("Office", 10));
        givenNodeCount(10, 1L);

        var result = new SchemaProcedure(internalTransaction, procedureMemory, procedureMemory.newTracker())
                .buildSchemaGraph(false);

        assertThat(result.nodes()).hasSize(1);
        assertThat(result.nodes().getFirst().getAllProperties())
                .containsEntry("name", "Office")
                .containsEntry("indexes", List.of())
                .containsEntry("constraints", List.of());
        assertThat(result.relationships()).isEmpty();
    }

    @Test
    void shouldIncludeTripleVerifiedByRelationshipTypeScan() throws KernelException {
        givenLabels(new NamedToken("A", 10), new NamedToken("B", 20));
        givenNodeCount(10, 10L);
        givenNodeCount(20, 10L);
        givenRelationshipType("R", 5);
        givenRelationshipCount(10, 5, TokenConstants.ANY_LABEL, 3L);
        givenRelationshipCount(TokenConstants.ANY_LABEL, 5, 20, 3L);
        givenRelationshipCount(TokenConstants.ANY_LABEL, 5, TokenConstants.ANY_LABEL, 1L);

        var relCursor = mock(RelationshipTypeIndexCursor.class);
        when(cursors.allocateRelationshipTypeIndexCursor(any(), any())).thenReturn(relCursor);
        when(relCursor.next()).thenReturn(true, false);
        when(relCursor.readFromStore()).thenReturn(true);
        when(relCursor.sourceNodeReference()).thenReturn(100L);
        when(relCursor.targetNodeReference()).thenReturn(200L);

        var nodeCursor = mock(NodeCursor.class);
        when(cursors.allocateNodeCursor(any(), any())).thenReturn(nodeCursor);
        var currentNode = new AtomicLong();
        doAnswer(invocation -> {
                    currentNode.set(invocation.getArgument(0));
                    return null;
                })
                .when(dataRead)
                .singleNode(anyLong(), any());
        when(nodeCursor.next()).thenReturn(true);
        when(nodeCursor.hasLabel(anyInt())).thenAnswer(invocation -> {
            int label = invocation.getArgument(0);
            long node = currentNode.get();
            return (node == 100L && label == 10) || (node == 200L && label == 20);
        });

        var result = new SchemaProcedure(internalTransaction, procedureMemory, procedureMemory.newTracker())
                .buildSchemaGraph(false);

        assertThat(result.nodes()).hasSize(2);
        assertThat(result.relationships()).hasSize(1);
        assertThat(result.relationships().getFirst().getType().name()).isEqualTo("R");
    }

    @Test
    void shouldExcludeGhostTripleWhenScanFindsNothing() throws KernelException {
        givenLabels(new NamedToken("A", 10), new NamedToken("B", 20));
        givenNodeCount(10, 10L);
        givenNodeCount(20, 10L);
        givenRelationshipType("R", 5);
        givenRelationshipCount(10, 5, TokenConstants.ANY_LABEL, 3L);
        givenRelationshipCount(TokenConstants.ANY_LABEL, 5, 20, 3L);
        givenRelationshipCount(TokenConstants.ANY_LABEL, 5, TokenConstants.ANY_LABEL, 1L);

        var relCursor = mock(RelationshipTypeIndexCursor.class);
        when(cursors.allocateRelationshipTypeIndexCursor(any(), any())).thenReturn(relCursor);
        when(relCursor.next()).thenReturn(false);

        when(cursors.allocateNodeCursor(any(), any())).thenReturn(mock(NodeCursor.class));

        var result = new SchemaProcedure(internalTransaction, procedureMemory, procedureMemory.newTracker())
                .buildSchemaGraph(false);

        assertThat(result.nodes()).hasSize(2);
        assertThat(result.relationships()).isEmpty();
    }

    @Test
    void shouldIncludeTripleVerifiedByNodeLabelScan() throws KernelException {
        givenLabels(new NamedToken("A", 10), new NamedToken("B", 20));
        givenNodeCount(10, 1L);
        givenNodeCount(20, 10L);
        givenRelationshipType("R", 5);
        givenRelationshipCount(10, 5, TokenConstants.ANY_LABEL, 1L);
        givenRelationshipCount(TokenConstants.ANY_LABEL, 5, 20, 10L);
        givenRelationshipCount(TokenConstants.ANY_LABEL, 5, TokenConstants.ANY_LABEL, 10L);

        var labelCursor = mock(NodeLabelIndexCursor.class);
        when(cursors.allocateNodeLabelIndexCursor(any(), any())).thenReturn(labelCursor);
        when(labelCursor.next()).thenReturn(true, false);
        when(labelCursor.nodeReference()).thenReturn(100L);

        var traversalCursor = mock(RelationshipTraversalCursor.class);
        when(cursors.allocateRelationshipTraversalCursor(any(), any())).thenReturn(traversalCursor);
        when(traversalCursor.next()).thenReturn(true, false);
        when(traversalCursor.otherNodeReference()).thenReturn(200L);

        var nodeCursor = mock(NodeCursor.class);
        when(cursors.allocateNodeCursor(any(), any())).thenReturn(nodeCursor);
        var currentNode = new AtomicLong();
        doAnswer(invocation -> {
                    currentNode.set(invocation.getArgument(0));
                    return null;
                })
                .when(dataRead)
                .singleNode(anyLong(), any());
        when(nodeCursor.next()).thenReturn(true);
        when(nodeCursor.hasLabel(anyInt())).thenAnswer(invocation -> {
            int label = invocation.getArgument(0);
            long node = currentNode.get();
            return (node == 100L && label == 10) || (node == 200L && label == 20);
        });

        var result = new SchemaProcedure(internalTransaction, procedureMemory, procedureMemory.newTracker())
                .buildSchemaGraph(false);

        assertThat(result.nodes()).hasSize(2);
        assertThat(result.relationships()).hasSize(1);
        assertThat(result.relationships().getFirst().getType().name()).isEqualTo("R");
    }
}
