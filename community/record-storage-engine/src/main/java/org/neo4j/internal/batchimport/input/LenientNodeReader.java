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
package org.neo4j.internal.batchimport.input;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

import java.io.IOException;
import org.neo4j.common.EntityType;
import org.neo4j.internal.batchimport.ReadBehaviour;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

class LenientNodeReader extends LenientStoreInputChunk {
    private final NodeStore nodeStore;
    private final NodeRecord record;
    private final boolean compactNodeStore;

    LenientNodeReader(
            ReadBehaviour readBehaviour,
            NodeStore nodeStore,
            PropertyStore propertyStore,
            TokenHolders tokenHolders,
            CursorContextFactory cursorFactory,
            StoreCursors storeCursors,
            boolean compactNodeStore,
            Group group) {
        super(
                readBehaviour,
                propertyStore,
                tokenHolders,
                cursorFactory,
                storeCursors,
                storeCursors.readCursor(NODE_CURSOR),
                group);
        this.nodeStore = nodeStore;
        this.record = nodeStore.newRecord();
        this.compactNodeStore = compactNodeStore;
    }

    @Override
    void readAndVisit(long id, InputEntityVisitor visitor, StoreCursors storeCursors) throws IOException {
        nodeStore.getRecordByCursor(id, record, RecordLoad.LENIENT_CHECK, cursor);
        if (record.inUse()) {
            nodeStore.ensureHeavy(record, storeCursors);
            int[] labelIds = parseLabelsField(record).get(nodeStore, storeCursors);
            String[] labels = toNames(tokenHolders.labelTokens(), labelIds);
            if (readBehaviour.shouldIncludeNode(labels)) {
                labels = readBehaviour.filterLabels(labels);
                if (compactNodeStore) {
                    // this is the variant where the target store will generate new IDs
                    visitor.id(id, group);
                } else {
                    // this is the variant where read node ID will map 1-to-1 with created node ID
                    visitor.id(id, group, cursorContext -> id);
                }
                visitor.labels(labels);
                visitPropertyChainNoThrow(visitor, record, EntityType.NODE, labels);
                visitor.endOfEntity();
            }
        } else {
            readBehaviour.unused();
        }
    }

    private String[] toNames(TokenHolder labelTokens, int[] labelIds) {
        String[] names = new String[labelIds.length];
        for (int i = 0; i < labelIds.length; i++) {
            names[i] =
                    LenientStoreInput.getTokenByIdSafe(labelTokens, labelIds[i]).name();
        }
        return names;
    }

    @Override
    String recordType() {
        return "Node";
    }

    @Override
    boolean shouldIncludeProperty(ReadBehaviour readBehaviour, String key, String[] owningEntityTokens) {
        return readBehaviour.shouldIncludeNodeProperty(key, owningEntityTokens, true);
    }
}
