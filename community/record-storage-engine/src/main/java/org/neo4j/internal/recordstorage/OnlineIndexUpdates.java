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
package org.neo4j.internal.recordstorage;

import static org.neo4j.internal.recordstorage.Command.Mode.CREATE;
import static org.neo4j.internal.recordstorage.Command.Mode.DELETE;
import static org.neo4j.io.IOUtils.closeAllUnchecked;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.neo4j.common.EntityType;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.recordstorage.EntityCommandGrouper.Cursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.util.VisibleForTesting;

/**
 * Derives logical index updates from physical records, provided by {@link NodeCommand node commands},
 * {@link RelationshipCommand relationship commands} and {@link PropertyCommand property commands}. For some
 * types of updates state from store is also needed, for example if adding a label to a node which already has
 * properties matching existing and online indexes; in that case the properties for that node needs to be read
 * from store since the commands in that transaction cannot itself provide enough information.
 *
 * One instance can be {@link IndexUpdates#feed(Cursor, Cursor, CommandSelector) fed} data about
 * multiple transactions, to be {@link #iterator() accessed} later.
 */
public class OnlineIndexUpdates implements IndexUpdates {
    private final NodeStore nodeStore;
    private final SchemaCache schemaCache;
    private final PropertyPhysicalToLogicalConverter converter;
    private final StorageReader reader;
    private final CursorContext cursorContext;
    private final MemoryTracker memoryTracker;
    private final StoreCursors storeCursors;
    private final Collection<IndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();
    private StorageNodeCursor nodeCursor;
    private StorageRelationshipScanCursor relationshipCursor;

    public OnlineIndexUpdates(
            NodeStore nodeStore,
            SchemaCache schemaCache,
            PropertyPhysicalToLogicalConverter converter,
            StorageReader reader,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            StoreCursors storeCursors) {
        this.nodeStore = nodeStore;
        this.schemaCache = schemaCache;
        this.converter = converter;
        this.reader = reader;
        this.cursorContext = cursorContext;
        this.memoryTracker = memoryTracker;
        this.storeCursors = storeCursors;
    }

    @Override
    public Iterator<IndexEntryUpdate<IndexDescriptor>> iterator() {
        return updates.iterator();
    }

    @Override
    public void feed(
            EntityCommandGrouper<NodeCommand>.Cursor nodeCommands,
            EntityCommandGrouper<RelationshipCommand>.Cursor relationshipCommands,
            CommandSelector commandSelector) {
        while (nodeCommands.nextEntity()) {
            gatherUpdatesFor(
                    nodeCommands.currentEntityId(), nodeCommands.currentEntityCommand(), nodeCommands, commandSelector);
        }
        while (relationshipCommands.nextEntity()) {
            gatherUpdatesFor(
                    relationshipCommands.currentEntityId(),
                    relationshipCommands.currentEntityCommand(),
                    relationshipCommands,
                    commandSelector);
        }
    }

    @Override
    public boolean hasUpdates() {
        return !updates.isEmpty();
    }

    private void gatherUpdatesFor(
            long nodeId,
            NodeCommand nodeCommand,
            EntityCommandGrouper<NodeCommand>.Cursor propertyCommands,
            CommandSelector commandSelector) {
        EntityUpdates nodeUpdates =
                gatherUpdatesFromCommandsForNode(nodeId, nodeCommand, propertyCommands, commandSelector);
        eagerlyGatherValueIndexUpdates(nodeUpdates, EntityType.NODE);
        eagerlyGatherTokenIndexUpdates(nodeUpdates, EntityType.NODE);
    }

    private void gatherUpdatesFor(
            long relationshipId,
            RelationshipCommand relationshipCommand,
            EntityCommandGrouper<RelationshipCommand>.Cursor propertyCommands,
            CommandSelector commandSelector) {
        EntityUpdates relationshipUpdates = gatherUpdatesFromCommandsForRelationship(
                relationshipId, relationshipCommand, propertyCommands, commandSelector);
        eagerlyGatherValueIndexUpdates(relationshipUpdates, EntityType.RELATIONSHIP);
        eagerlyGatherTokenIndexUpdates(relationshipUpdates, EntityType.RELATIONSHIP);
    }

    private void eagerlyGatherValueIndexUpdates(EntityUpdates entityUpdates, EntityType entityType) {
        Iterable<IndexDescriptor> relatedIndexes = schemaCache.getValueIndexesRelatedTo(
                entityUpdates.entityTokensChanged(),
                entityUpdates.entityTokensUnchanged(),
                entityUpdates.propertiesChanged(),
                entityUpdates.isPropertyListComplete(),
                entityType);
        // we need to materialize the IndexEntryUpdates here, because when we
        // consume (later in separate thread) the store might have changed.
        entityUpdates
                .valueUpdatesForIndexKeys(
                        relatedIndexes, reader, entityType, cursorContext, storeCursors, memoryTracker)
                .forEach(updates::add);
    }

    private EntityUpdates gatherUpdatesFromCommandsForNode(
            long nodeId,
            NodeCommand nodeChanges,
            EntityCommandGrouper<NodeCommand>.Cursor propertyCommandsForNode,
            CommandSelector commandSelector) {
        int[] nodeLabelsBefore;
        int[] nodeLabelsAfter;
        if (nodeChanges != null) {
            // Special case since the node may not be heavy, i.e. further loading may be required
            nodeLabelsBefore = NodeLabelsField.getNoEnsureHeavy(
                    commandSelector.getBefore(nodeChanges), nodeStore, storeCursors, memoryTracker);
            nodeLabelsAfter = NodeLabelsField.getNoEnsureHeavy(
                    commandSelector.getAfter(nodeChanges), nodeStore, storeCursors, memoryTracker);
        } else {
            /* If the node doesn't exist here then we've most likely encountered this scenario:
             * - TX1: Node N exists and has property record P
             * - rotate log
             * - TX2: P gets changed
             * - TX3: N gets deleted (also P, but that's irrelevant for this scenario)
             * - N is persisted to disk for some reason
             * - crash
             * - recover
             * - TX2: P has changed and updates to indexes are gathered. As part of that it tries to read
             *        the labels of N (which does not exist a.t.m.).
             *
             * We can actually (if we disregard any potential inconsistencies) just assume that
             * if this happens and we're in recovery mode that the node in question will be deleted
             * in an upcoming transaction, so just skip this update.
             */
            StorageNodeCursor nodeCursor = loadNode(nodeId);
            nodeLabelsBefore = nodeLabelsAfter = nodeCursor.labels();
        }

        // First get possible Label changes
        boolean complete = providesCompleteListOfProperties(nodeChanges);
        EntityUpdates.Builder nodePropertyUpdates = EntityUpdates.forEntity(nodeId, complete)
                .withTokensBefore(nodeLabelsBefore)
                .withTokensAfter(nodeLabelsAfter);

        // Then look for property changes
        converter.convertPropertyRecord(propertyCommandsForNode, nodePropertyUpdates, commandSelector);
        return nodePropertyUpdates.build();
    }

    private void eagerlyGatherTokenIndexUpdates(EntityUpdates entityUpdates, EntityType entityType) {
        IndexDescriptor relatedToken =
                schemaCache.indexForSchemaAndType(SchemaDescriptors.forAnyEntityTokens(entityType), IndexType.LOOKUP);
        entityUpdates.tokenUpdateForIndexKey(relatedToken).ifPresent(updates::add);
    }

    private static boolean providesCompleteListOfProperties(Command entityCommand) {
        return entityCommand != null && (entityCommand.getMode() == CREATE || entityCommand.getMode() == DELETE);
    }

    private EntityUpdates gatherUpdatesFromCommandsForRelationship(
            long relationshipId,
            RelationshipCommand relationshipCommand,
            EntityCommandGrouper<RelationshipCommand>.Cursor propertyCommands,
            CommandSelector commandSelector) {
        int reltypeBefore;
        int reltypeAfter;
        if (relationshipCommand != null) {
            reltypeBefore = commandSelector.getBefore(relationshipCommand).getType();
            reltypeAfter = commandSelector.getAfter(relationshipCommand).getType();
        } else {
            reltypeAfter = loadRelationship(relationshipId).type();
            reltypeBefore = reltypeAfter;
        }
        boolean complete = providesCompleteListOfProperties(relationshipCommand);
        var relationshipPropertyUpdates = EntityUpdates.forEntity(relationshipId, complete);
        if (reltypeBefore != TokenConstants.NO_TOKEN) {
            relationshipPropertyUpdates.withTokensBefore(reltypeBefore);
        }
        if (reltypeAfter != TokenConstants.NO_TOKEN) {
            relationshipPropertyUpdates.withTokensAfter(reltypeAfter);
        }

        converter.convertPropertyRecord(propertyCommands, relationshipPropertyUpdates, commandSelector);
        return relationshipPropertyUpdates.build();
    }

    private StorageNodeCursor loadNode(long nodeId) {
        if (nodeCursor == null) {
            nodeCursor = reader.allocateNodeCursor(cursorContext, storeCursors, memoryTracker);
        }
        nodeCursor.single(nodeId);
        if (!nodeCursor.next()) {
            throw new IllegalStateException("Node[" + nodeId + "] doesn't exist");
        }
        return nodeCursor;
    }

    private StorageRelationshipScanCursor loadRelationship(long relationshipId) {
        if (relationshipCursor == null) {
            relationshipCursor = reader.allocateRelationshipScanCursor(cursorContext, storeCursors, memoryTracker);
        }
        relationshipCursor.single(relationshipId);
        if (!relationshipCursor.next()) {
            throw new IllegalStateException("Relationship[" + relationshipId + "] doesn't exist");
        }
        return relationshipCursor;
    }

    @Override
    public void close() {
        closeAllUnchecked(nodeCursor, relationshipCursor, reader);
    }

    @Override
    public void reset() {
        updates.clear();
    }

    @VisibleForTesting
    protected Collection<IndexEntryUpdate<IndexDescriptor>> getUpdates() {
        return updates;
    }

    @Override
    public String toString() {
        return "OnlineIndexUpdates[" + updates + "]";
    }
}
