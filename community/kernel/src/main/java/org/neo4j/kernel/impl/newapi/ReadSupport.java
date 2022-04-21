/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class ReadSupport {
    private final StorageReader storageReader;
    private final DefaultPooledCursors cursors;
    private final org.neo4j.internal.kernel.api.Read readDelegate;

    public ReadSupport(
            StorageReader storageReader,
            DefaultPooledCursors cursors,
            org.neo4j.internal.kernel.api.Read readDelegate) {
        this.storageReader = storageReader;
        this.cursors = cursors;
        this.readDelegate = readDelegate;
    }

    public boolean nodeExistsWithoutTxState(
            long reference, AccessMode accessMode, StoreCursors storeCursors, CursorContext cursorContext) {
        boolean existsInNodeStore = storageReader.nodeExists(reference, storeCursors);

        if (accessMode.allowsTraverseAllLabels()) {
            return existsInNodeStore;
        } else if (!existsInNodeStore) {
            return false;
        } else {
            try (DefaultNodeCursor node = cursors.allocateNodeCursor(cursorContext)) {
                readDelegate.singleNode(reference, node);
                return node.next();
            }
        }
    }

    public boolean relationshipExistsWithoutTx(
            long reference, AccessMode accessMode, StoreCursors storeCursors, CursorContext cursorContext) {
        boolean existsInRelStore = storageReader.relationshipExists(reference, storeCursors);
        if (accessMode.allowsTraverseAllRelTypes()) {
            return existsInRelStore;
        } else if (!existsInRelStore) {
            return false;
        } else {
            try (DefaultRelationshipScanCursor rels = cursors.allocateRelationshipScanCursor(cursorContext)) {
                readDelegate.singleRelationship(reference, rels);
                return rels.next();
            }
        }
    }
}
