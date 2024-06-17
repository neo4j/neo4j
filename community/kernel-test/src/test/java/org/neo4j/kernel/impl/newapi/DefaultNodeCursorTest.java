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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;

class DefaultNodeCursorTest {
    private final InternalCursorFactory internalCursors = MockedInternalCursors.mockedInternalCursors();

    @Test
    void hasLabelOnNewNodeDoesNotTouchStore() {
        final var NODEID = 1L;
        var read = buildReadState(txState -> txState.nodeDoCreate(NODEID));

        var storageCursor = mock(StorageNodeCursor.class);
        try (var defaultCursor = new DefaultNodeCursor((c) -> {}, storageCursor, internalCursors, false)) {
            defaultCursor.single(NODEID, read);
            final TestKernelReadTracer tracer = addTracerAndReturn(defaultCursor);

            assertTrue(defaultCursor.next());
            tracer.clear();

            assertFalse(defaultCursor.hasLabel());
            // Should not have touched the store to verify that no label exists
            verify(storageCursor, never()).hasLabel();
            // Verify that the tracer captured the event
            tracer.assertEvents(TestKernelReadTracer.hasLabelEvent());
        }
    }

    @Test
    void hasSpecifiedLabelOnNewNodeDoesNotTouchStore() {
        final var NODEID = 1L;
        var read = buildReadState(txState -> txState.nodeDoCreate(NODEID));

        var storageCursor = mock(StorageNodeCursor.class);
        try (var defaultCursor = new DefaultNodeCursor((c) -> {}, storageCursor, internalCursors, false)) {
            final TestKernelReadTracer tracer = addTracerAndReturn(defaultCursor);
            defaultCursor.single(NODEID, read);
            assertTrue(defaultCursor.next());
            tracer.clear();

            assertFalse(defaultCursor.hasLabel(7));
            // Should not have touched the store to verify that the label exists
            verify(storageCursor, never()).hasLabel(7);
            // Verify that the tracer captured the event
            tracer.assertEvents(TestKernelReadTracer.hasLabelEvent(7));
        }
    }

    private static Read buildReadState(Consumer<TxState> setup) {
        var ktx = mock(KernelTransactionImplementation.class);
        when(ktx.securityContext()).thenReturn(SecurityContext.AUTH_DISABLED);
        var read = new AllStoreHolder.ForTransactionScope(
                mock(StorageReader.class),
                mock(TokenRead.class),
                ktx,
                mock(Locks.class),
                mock(DefaultPooledCursors.class),
                mock(IndexingService.class),
                EmptyMemoryTracker.INSTANCE,
                false,
                mock(QueryContext.class),
                mock(AssertOpen.class),
                mock(SchemaRead.class));
        var txState = new TxState();
        setup.accept(txState);
        when(ktx.hasTxStateWithChanges()).thenReturn(true);
        when(ktx.txState()).thenReturn(txState);
        return read;
    }

    private static TestKernelReadTracer addTracerAndReturn(DefaultNodeCursor nodeCursor) {
        final TestKernelReadTracer tracer = new TestKernelReadTracer();
        nodeCursor.setTracer(tracer);
        return tracer;
    }
}
