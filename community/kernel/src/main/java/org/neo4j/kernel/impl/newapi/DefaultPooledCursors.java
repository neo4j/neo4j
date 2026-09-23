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

import static org.neo4j.util.FeatureToggles.flag;

import java.util.ArrayList;
import java.util.Arrays;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.lang.AutoCloseablePlus;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Neo4jInternalErrorLogMessage;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.log4j.Neo4jLogMarkers;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Cursor factory which pools 1 cursor of each kind. Not thread-safe at all.
 */
public class DefaultPooledCursors extends DefaultCursors implements CursorFactory {
    private static final boolean FAIL_ON_LEAKED_CURSORS =
            flag(DefaultPooledCursors.class, "FAIL_ON_LEAKED_CURSORS", false);

    // Enums for cursor leak reporting
    private enum CursorKind {
        NODE("node"),
        FULL_NODE("full access node"),
        RELATIONSHIP_SCAN("relationship scan"),
        FULL_RELATIONSHIP_SCAN("full access relationship scan"),
        RELATIONSHIP_TRAVERSAL("relationship traversal"),
        FULL_RELATIONSHIP_TRAVERSAL("full access relationship traversal"),
        PROPERTY("property"),
        FULL_PROPERTY("full access property"),
        NODE_VALUE_INDEX("node value index"),
        NODE_LABEL_INDEX("node label index"),
        FULL_NODE_LABEL_INDEX("full access node label index"),
        RELATIONSHIP_VALUE_INDEX("relationship value index"),
        RELATIONSHIP_TYPE_INDEX("relationship type index"),
        FULL_RELATIONSHIP_TYPE_INDEX("full access relationship type index");

        private static final CursorKind[] VALUES = values();

        private final String name;

        CursorKind(String name) {
            this.name = name;
        }

        String displayName() {
            return name;
        }
    }

    private final StorageReader storageReader;
    private final StoreCursors storeCursors;
    private final StorageEngineIndexingBehaviour indexingBehaviour;
    private final boolean applyAccessModeToTxState;
    private final DefaultCursorFactory cursorFactory;
    private final boolean failOnLeakedCursors;
    private final boolean logLeakedCursors;
    private final boolean trackLeakedCursors;
    private final InternalLog log;
    private final int[] numNonReturnedCursors = new int[CursorKind.VALUES.length];
    private DefaultNodeCursor nodeCursor;
    private DefaultNodeCursor fullAccessNodeCursor;
    private DefaultRelationshipScanCursor relationshipScanCursor;
    private DefaultRelationshipScanCursor fullAccessRelationshipScanCursor;
    private DefaultRelationshipTraversalCursor relationshipTraversalCursor;
    private DefaultRelationshipTraversalCursor fullAccessRelationshipTraversalCursor;
    private TraceablePropertyCursor propertyCursor;
    private PropertyCursor fullAccessPropertyCursor;
    private DefaultNodeValueIndexCursor nodeValueIndexCursor;
    private DefaultNodeLabelIndexCursor nodeLabelIndexCursor;
    private DefaultRelationshipValueIndexCursor relationshipValueIndexCursor;
    private DefaultNodeLabelIndexCursor fullAccessNodeLabelIndexCursor;
    private InternalRelationshipTypeIndexCursor relationshipTypeIndexCursor;
    private InternalRelationshipTypeIndexCursor fullAccessRelationshipTypeIndexCursor;

    public DefaultPooledCursors(
            StorageReader storageReader,
            StoreCursors storeCursors,
            Config config,
            StorageEngineIndexingBehaviour indexingBehaviour,
            boolean applyAccessModeToTxState) {
        this(
                storageReader,
                storeCursors,
                config,
                indexingBehaviour,
                applyAccessModeToTxState,
                DefaultCursorFactory.DEFAULT,
                NullLog.getInstance());
    }

    public DefaultPooledCursors(
            StorageReader storageReader,
            StoreCursors storeCursors,
            Config config,
            StorageEngineIndexingBehaviour indexingBehaviour,
            boolean applyAccessModeToTxState,
            InternalLog log) {
        this(
                storageReader,
                storeCursors,
                config,
                indexingBehaviour,
                applyAccessModeToTxState,
                DefaultCursorFactory.DEFAULT,
                log);
    }

    protected DefaultPooledCursors(
            StorageReader storageReader,
            StoreCursors storeCursors,
            Config config,
            StorageEngineIndexingBehaviour indexingBehaviour,
            boolean applyAccessModeToTxState,
            DefaultCursorFactory cursorFactory,
            InternalLog log) {
        super(new ArrayList<>(), config);
        this.storageReader = storageReader;
        this.storeCursors = storeCursors;
        this.indexingBehaviour = indexingBehaviour;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
        this.cursorFactory = cursorFactory;
        this.failOnLeakedCursors = config.get(GraphDatabaseInternalSettings.fail_on_leaked_cursors)
                || (FAIL_ON_LEAKED_CURSORS
                        && !config.isExplicitlySet(GraphDatabaseInternalSettings.fail_on_leaked_cursors));
        this.logLeakedCursors = config.get(GraphDatabaseInternalSettings.log_leaked_cursors);
        this.trackLeakedCursors = failOnLeakedCursors || logLeakedCursors;
        this.log = log;
    }

    private <T extends AutoCloseablePlus> T trace(T cursor, CursorKind kind) {
        T tracedCursor = trace(cursor);
        if (trackLeakedCursors) {
            numNonReturnedCursors[kind.ordinal()]++;
        }
        return tracedCursor;
    }

    @Override
    public DefaultNodeCursor allocateNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (nodeCursor == null) {
            return trace(newNodeCursor(cursorContext, memoryTracker), CursorKind.NODE);
        }
        try {
            return acquire(nodeCursor, CursorKind.NODE);
        } finally {
            nodeCursor = null;
        }
    }

    private DefaultNodeCursor newNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return cursorFactory.nodeCursor(
                this::accept,
                storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker),
                newInternalCursors(cursorContext, memoryTracker),
                applyAccessModeToTxState);
    }

    private <T extends TraceableCursor> T acceptCursor(T pooledCursor, T cursor, CursorKind kind) {
        if (pooledCursor != null) {
            pooledCursor.release();
        }
        cursor.removeTracer();
        if (trackLeakedCursors) {
            if (numNonReturnedCursors[kind.ordinal()] == 0) {
                reportOverReturnedCursor(kind);
            } else {
                numNonReturnedCursors[kind.ordinal()]--;
            }
        }
        return cursor;
    }

    private void reportOverReturnedCursor(CursorKind kind) {
        String message = "Returned too many cursors to the pool, kind:" + kind.displayName();
        if (logLeakedCursors) {
            log.warn(new Neo4jInternalErrorLogMessage(Neo4jLogMarkers.KERNEL, message, null));
        }
        if (failOnLeakedCursors) {
            throw new IllegalStateException(message);
        }
    }

    protected void accept(DefaultNodeCursor cursor) {
        nodeCursor = acceptCursor(nodeCursor, cursor, CursorKind.NODE);
    }

    @Override
    public DefaultNodeCursor allocateFullAccessNodeCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessNodeCursor == null) {
            return trace(
                    cursorFactory.fullAccessNodeCursor(
                            this::acceptFullAccess,
                            storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker)),
                    CursorKind.FULL_NODE);
        }
        try {
            return acquire(fullAccessNodeCursor, CursorKind.FULL_NODE);
        } finally {
            fullAccessNodeCursor = null;
        }
    }

    private void acceptFullAccess(DefaultNodeCursor cursor) {
        fullAccessNodeCursor = acceptCursor(fullAccessNodeCursor, cursor, CursorKind.FULL_NODE);
    }

    @Override
    public RelationshipScanCursor allocateRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipScanCursor == null) {
            StorageRelationshipScanCursor storeCursor =
                    storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker);
            InternalCursorFactory internalCursors = newInternalCursors(cursorContext, memoryTracker);
            return trace(
                    cursorFactory.relationshipScanCursor(
                            this::accept, storeCursor, internalCursors, applyAccessModeToTxState),
                    CursorKind.RELATIONSHIP_SCAN);
        }
        try {
            return acquire(relationshipScanCursor, CursorKind.RELATIONSHIP_SCAN);
        } finally {
            relationshipScanCursor = null;
        }
    }

    private void accept(DefaultRelationshipScanCursor cursor) {
        relationshipScanCursor = acceptCursor(relationshipScanCursor, cursor, CursorKind.RELATIONSHIP_SCAN);
    }

    @Override
    public DefaultRelationshipScanCursor allocateFullAccessRelationshipScanCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipScanCursor == null) {
            return trace(
                    cursorFactory.fullAccessRelationshipScanCursor(
                            this::acceptFullAccess,
                            storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker)),
                    CursorKind.FULL_RELATIONSHIP_SCAN);
        }

        try {
            return acquire(fullAccessRelationshipScanCursor, CursorKind.FULL_RELATIONSHIP_SCAN);
        } finally {
            fullAccessRelationshipScanCursor = null;
        }
    }

    private <C extends TraceableCursor> C acquire(C cursor, CursorKind kind) {
        cursor.acquire();
        if (trackLeakedCursors) {
            numNonReturnedCursors[kind.ordinal()]++;
        }
        return cursor;
    }

    private void acceptFullAccess(DefaultRelationshipScanCursor cursor) {
        fullAccessRelationshipScanCursor =
                acceptCursor(fullAccessRelationshipScanCursor, cursor, CursorKind.FULL_RELATIONSHIP_SCAN);
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipTraversalCursor == null) {
            return trace(
                    cursorFactory.relationshipTraversalCursor(
                            this::accept,
                            storageReader.allocateRelationshipTraversalCursor(
                                    cursorContext, storeCursors, memoryTracker),
                            newInternalCursors(cursorContext, memoryTracker),
                            applyAccessModeToTxState),
                    CursorKind.RELATIONSHIP_TRAVERSAL);
        }

        try {
            return acquire(relationshipTraversalCursor, CursorKind.RELATIONSHIP_TRAVERSAL);
        } finally {
            relationshipTraversalCursor = null;
        }
    }

    private void accept(DefaultRelationshipTraversalCursor cursor) {
        relationshipTraversalCursor =
                acceptCursor(relationshipTraversalCursor, cursor, CursorKind.RELATIONSHIP_TRAVERSAL);
    }

    @Override
    public RelationshipTraversalCursor allocateFullAccessRelationshipTraversalCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipTraversalCursor == null) {
            return trace(
                    cursorFactory.fullAccessRelationshipTraversalCursor(
                            this::acceptFullAccess,
                            storageReader.allocateRelationshipTraversalCursor(
                                    cursorContext, storeCursors, memoryTracker)),
                    CursorKind.FULL_RELATIONSHIP_TRAVERSAL);
        }

        try {
            return acquire(fullAccessRelationshipTraversalCursor, CursorKind.FULL_RELATIONSHIP_TRAVERSAL);
        } finally {
            fullAccessRelationshipTraversalCursor = null;
        }
    }

    private void acceptFullAccess(DefaultRelationshipTraversalCursor cursor) {
        fullAccessRelationshipTraversalCursor =
                acceptCursor(fullAccessRelationshipTraversalCursor, cursor, CursorKind.FULL_RELATIONSHIP_TRAVERSAL);
    }

    @Override
    public PropertyCursor allocatePropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (propertyCursor == null) {
            return trace(
                    cursorFactory.propertyCursor(
                            this::accept,
                            storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker),
                            newInternalCursors(cursorContext, memoryTracker),
                            applyAccessModeToTxState),
                    CursorKind.PROPERTY);
        }
        try {
            return acquire(propertyCursor, CursorKind.PROPERTY);
        } finally {
            propertyCursor = null;
        }
    }

    private void accept(TraceablePropertyCursor cursor) {
        propertyCursor = acceptCursor(propertyCursor, cursor, CursorKind.PROPERTY);
    }

    @Override
    public PropertyCursor allocateFullAccessPropertyCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessPropertyCursor == null) {
            return trace(
                    cursorFactory.fullAccessPropertyCursor(
                            this::acceptFullAccess,
                            storageReader.allocatePropertyCursor(cursorContext, storeCursors, memoryTracker)),
                    CursorKind.FULL_PROPERTY);
        }

        try {
            return acquire((TraceableCursor & PropertyCursor) fullAccessPropertyCursor, CursorKind.FULL_PROPERTY);
        } finally {
            fullAccessPropertyCursor = null;
        }
    }

    private void acceptFullAccess(PropertyCursor cursor) {
        fullAccessPropertyCursor = acceptCursor(
                (TraceablePropertyCursor & PropertyCursor) fullAccessPropertyCursor,
                (TraceablePropertyCursor & PropertyCursor) cursor,
                CursorKind.FULL_PROPERTY);
    }

    @Override
    public NodeValueIndexCursor allocateNodeValueIndexCursor(CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (nodeValueIndexCursor == null) {
            return trace(
                    cursorFactory.nodeValueIndexCursor(
                            this::accept, newInternalCursors(cursorContext, memoryTracker), applyAccessModeToTxState),
                    CursorKind.NODE_VALUE_INDEX);
        }

        try {
            return acquire(nodeValueIndexCursor, CursorKind.NODE_VALUE_INDEX);
        } finally {
            nodeValueIndexCursor = null;
        }
    }

    private void accept(DefaultNodeValueIndexCursor cursor) {
        nodeValueIndexCursor = acceptCursor(nodeValueIndexCursor, cursor, CursorKind.NODE_VALUE_INDEX);
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateNodeLabelIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (nodeLabelIndexCursor == null) {
            return trace(
                    cursorFactory.nodeLabelIndexCursor(
                            this::accept, newInternalCursors(cursorContext, memoryTracker), applyAccessModeToTxState),
                    CursorKind.NODE_LABEL_INDEX);
        }

        try {
            return acquire(nodeLabelIndexCursor, CursorKind.NODE_LABEL_INDEX);
        } finally {
            nodeLabelIndexCursor = null;
        }
    }

    private void accept(DefaultNodeLabelIndexCursor cursor) {
        nodeLabelIndexCursor = acceptCursor(nodeLabelIndexCursor, cursor, CursorKind.NODE_LABEL_INDEX);
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateFullAccessNodeLabelIndexCursor(CursorContext cursorContext) {
        if (fullAccessNodeLabelIndexCursor == null) {
            return trace(
                    cursorFactory.fullAccessNodeLabelIndexCursor(this::acceptFullAccess),
                    CursorKind.FULL_NODE_LABEL_INDEX);
        }

        try {
            return acquire(fullAccessNodeLabelIndexCursor, CursorKind.FULL_NODE_LABEL_INDEX);
        } finally {
            fullAccessNodeLabelIndexCursor = null;
        }
    }

    private void acceptFullAccess(DefaultNodeLabelIndexCursor cursor) {
        fullAccessNodeLabelIndexCursor =
                acceptCursor(fullAccessNodeLabelIndexCursor, cursor, CursorKind.FULL_NODE_LABEL_INDEX);
    }

    @Override
    public RelationshipValueIndexCursor allocateRelationshipValueIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipValueIndexCursor == null) {
            var internalCursors = newInternalCursors(cursorContext, memoryTracker);
            StorageRelationshipScanCursor storeCursor =
                    storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker);
            DefaultRelationshipScanCursor relationshipScanCursor = cursorFactory.relationshipScanCursor(
                    c -> {}, storeCursor, internalCursors, applyAccessModeToTxState);
            return trace(
                    cursorFactory.relationshipValueIndexCursor(
                            this::accept, relationshipScanCursor, internalCursors, applyAccessModeToTxState),
                    CursorKind.RELATIONSHIP_VALUE_INDEX);
        }

        try {
            return acquire(relationshipValueIndexCursor, CursorKind.RELATIONSHIP_VALUE_INDEX);
        } finally {
            relationshipValueIndexCursor = null;
        }
    }

    public void accept(DefaultRelationshipValueIndexCursor cursor) {
        relationshipValueIndexCursor =
                acceptCursor(relationshipValueIndexCursor, cursor, CursorKind.RELATIONSHIP_VALUE_INDEX);
    }

    @Override
    public RelationshipTypeIndexCursor allocateRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (relationshipTypeIndexCursor == null) {
            var internalCursors = newInternalCursors(cursorContext, memoryTracker);
            if (indexingBehaviour.useNodeIdsInRelationshipTokenIndex()) {
                var relationshipTraversalCursor = cursorFactory.relationshipTraversalCursor(
                        c -> {},
                        storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker),
                        internalCursors,
                        applyAccessModeToTxState);

                return trace(
                        cursorFactory.nodeBasedRelationshipTypeIndexCursor(
                                this::accept, internalCursors.allocateNodeCursor(), relationshipTraversalCursor),
                        CursorKind.RELATIONSHIP_TYPE_INDEX);
            } else {
                var relationshipScanCursor = cursorFactory.relationshipScanCursor(
                        c -> {},
                        storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker),
                        internalCursors,
                        applyAccessModeToTxState);
                return trace(
                        cursorFactory.relationshipBasedRelationshipTypeIndexCursor(
                                this::accept, relationshipScanCursor, applyAccessModeToTxState),
                        CursorKind.RELATIONSHIP_TYPE_INDEX);
            }
        }

        try {
            return acquire(relationshipTypeIndexCursor, CursorKind.RELATIONSHIP_TYPE_INDEX);
        } finally {
            relationshipTypeIndexCursor = null;
        }
    }

    private void accept(InternalRelationshipTypeIndexCursor cursor) {
        relationshipTypeIndexCursor =
                acceptCursor(relationshipTypeIndexCursor, cursor, CursorKind.RELATIONSHIP_TYPE_INDEX);
    }

    @Override
    public RelationshipTypeIndexCursor allocateFullAccessRelationshipTypeIndexCursor(
            CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (fullAccessRelationshipTypeIndexCursor == null) {
            if (indexingBehaviour.useNodeIdsInRelationshipTokenIndex()) {
                var nodeCursor = cursorFactory.fullAccessNodeCursor(
                        c -> {}, storageReader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker));
                var relationshipTraversalCursor = cursorFactory.fullAccessRelationshipTraversalCursor(
                        c -> {},
                        storageReader.allocateRelationshipTraversalCursor(cursorContext, storeCursors, memoryTracker));
                return trace(
                        cursorFactory.fullAccessNodeBasedRelationshipTypeIndexCursor(
                                this::acceptFullAccess, nodeCursor, relationshipTraversalCursor),
                        CursorKind.FULL_RELATIONSHIP_TYPE_INDEX);
            } else {

                var relationshipScanCursor = cursorFactory.fullAccessRelationshipScanCursor(
                        c -> {},
                        storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker));
                return trace(
                        cursorFactory.fullAccessRelationshipBasedRelationshipTypeIndexCursor(
                                this::acceptFullAccess, relationshipScanCursor),
                        CursorKind.FULL_RELATIONSHIP_TYPE_INDEX);
            }
        }

        try {
            return acquire(fullAccessRelationshipTypeIndexCursor, CursorKind.FULL_RELATIONSHIP_TYPE_INDEX);
        } finally {
            fullAccessRelationshipTypeIndexCursor = null;
        }
    }

    private void acceptFullAccess(InternalRelationshipTypeIndexCursor cursor) {
        fullAccessRelationshipTypeIndexCursor =
                acceptCursor(fullAccessRelationshipTypeIndexCursor, cursor, CursorKind.FULL_RELATIONSHIP_TYPE_INDEX);
    }

    public void release() {
        if (nodeCursor != null) {
            nodeCursor.release();
        }
        if (fullAccessNodeCursor != null) {
            fullAccessNodeCursor.release();
        }
        if (relationshipScanCursor != null) {
            relationshipScanCursor.release();
        }
        if (fullAccessRelationshipScanCursor != null) {
            fullAccessRelationshipScanCursor.release();
        }
        if (relationshipTraversalCursor != null) {
            relationshipTraversalCursor.release();
        }
        if (fullAccessRelationshipTraversalCursor != null) {
            fullAccessRelationshipTraversalCursor.release();
        }
        if (propertyCursor != null) {
            propertyCursor.release();
        }
        if (fullAccessPropertyCursor != null) {
            ((TraceableCursor) fullAccessPropertyCursor).release();
        }
        if (nodeValueIndexCursor != null) {
            nodeValueIndexCursor.release();
        }
        if (nodeLabelIndexCursor != null) {
            nodeLabelIndexCursor.release();
        }
        if (fullAccessNodeLabelIndexCursor != null) {
            fullAccessNodeLabelIndexCursor.release();
        }
        if (relationshipValueIndexCursor != null) {
            relationshipValueIndexCursor.release();
        }
        if (relationshipTypeIndexCursor != null) {
            relationshipTypeIndexCursor.release();
        }
        if (fullAccessRelationshipTypeIndexCursor != null) {
            fullAccessRelationshipTypeIndexCursor.release();
        }
        nodeCursor = null;
        fullAccessNodeCursor = null;
        relationshipScanCursor = null;
        fullAccessRelationshipScanCursor = null;
        relationshipTraversalCursor = null;
        fullAccessRelationshipTraversalCursor = null;
        propertyCursor = null;
        fullAccessPropertyCursor = null;
        nodeValueIndexCursor = null;
        nodeLabelIndexCursor = null;
        fullAccessNodeLabelIndexCursor = null;
        relationshipValueIndexCursor = null;
        relationshipTypeIndexCursor = null;
        fullAccessRelationshipTypeIndexCursor = null;

        if (trackLeakedCursors) {
            try {
                StringBuilder leaked = null;
                for (CursorKind kind : CursorKind.VALUES) {
                    int numNonReturned = numNonReturnedCursors[kind.ordinal()];
                    if (numNonReturned != 0) {
                        if (leaked == null) {
                            leaked = new StringBuilder();
                        } else {
                            leaked.append("; ");
                        }
                        leaked.append(kind.displayName()).append(": ").append(numNonReturned);
                    }
                }
                if (leaked != null) {
                    reportLeakedCursors(failOnLeakedCursors, logLeakedCursors, log, leaked.toString());
                }
            } finally {
                // We need to reset all the tracking so that leaks does not poison the potential reuse of the cursor
                // pool
                Arrays.fill(numNonReturnedCursors, 0);
            }
        }
    }

    static void reportLeakedCursors(
            boolean failOnLeakedCursors, boolean logLeakedCursors, InternalLog log, String leaked) {
        if (logLeakedCursors) {
            log.warn(new Neo4jInternalErrorLogMessage(Neo4jLogMarkers.KERNEL, leakMessage(leaked), null));
        }
        if (failOnLeakedCursors) {
            throw new IllegalStateException(leakMessage(leaked));
        }
    }

    private static String leakMessage(String leaked) {
        return "Not all allocated cursors were returned: " + leaked
                + ". Enable internal.dbms.debug.track_cursor_close and internal.dbms.debug.trace_cursors"
                + " to capture the allocating stack trace of the leaked cursor.";
    }

    private InternalCursorFactory newInternalCursors(CursorContext cursorContext, MemoryTracker memoryTracker) {
        return cursorFactory.internalCursors(
                storageReader, storeCursors, cursorContext, memoryTracker, applyAccessModeToTxState);
    }
}
