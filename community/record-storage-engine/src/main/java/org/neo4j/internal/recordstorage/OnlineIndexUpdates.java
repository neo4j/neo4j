/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.common.EntityType;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

import static org.neo4j.internal.recordstorage.Command.Mode.CREATE;
import static org.neo4j.internal.recordstorage.Command.Mode.DELETE;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Derives logical index updates from physical records, provided by {@link NodeCommand node commands},
 * {@link RelationshipCommand relationship commands} and {@link PropertyCommand property commands}. For some
 * types of updates state from store is also needed, for example if adding a label to a node which already has
 * properties matching existing and online indexes; in that case the properties for that node needs to be read
 * from store since the commands in that transaction cannot itself provide enough information.
 *
 * One instance can be {@link IndexUpdates#feed(EntityCommandGrouper.Cursor,EntityCommandGrouper.Cursor) fed} data about
 * multiple transactions, to be {@link #iterator() accessed} later.
 */
public class OnlineIndexUpdates implements IndexUpdates
{
    private final NodeStore nodeStore;
    private final SchemaCache schemaCache;
    private final PropertyPhysicalToLogicalConverter converter;
    private final StorageReader reader;
    private final Collection<IndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();
    private StorageNodeCursor nodeCursor;
    private StorageRelationshipScanCursor relationshipCursor;

    public OnlineIndexUpdates( NodeStore nodeStore, SchemaCache schemaCache,
            PropertyPhysicalToLogicalConverter converter, StorageReader reader )
    {
        this.nodeStore = nodeStore;
        this.schemaCache = schemaCache;
        this.converter = converter;
        this.reader = reader;
    }

    @Override
    public Iterator<IndexEntryUpdate<IndexDescriptor>> iterator()
    {
        return updates.iterator();
    }

    @Override
    public void feed( EntityCommandGrouper<NodeCommand>.Cursor nodeCommands, EntityCommandGrouper<RelationshipCommand>.Cursor relationshipCommands )
    {
        while ( nodeCommands.nextEntity() )
        {
            gatherUpdatesFor( nodeCommands.currentEntityId(), nodeCommands.currentEntityCommand(), nodeCommands );
        }
        while ( relationshipCommands.nextEntity() )
        {
            gatherUpdatesFor( relationshipCommands.currentEntityId(), relationshipCommands.currentEntityCommand(), relationshipCommands );
        }
    }

    @Override
    public boolean hasUpdates()
    {
        return !updates.isEmpty();
    }

    private void gatherUpdatesFor( long nodeId, NodeCommand nodeCommand, EntityCommandGrouper<NodeCommand>.Cursor propertyCommands )
    {
        EntityUpdates.Builder nodePropertyUpdate = gatherUpdatesFromCommandsForNode( nodeId, nodeCommand, propertyCommands );
        eagerlyGatherUpdates( nodePropertyUpdate, EntityType.NODE );
    }

    private void gatherUpdatesFor( long relationshipId, RelationshipCommand relationshipCommand,
            EntityCommandGrouper<RelationshipCommand>.Cursor propertyCommands )
    {
        EntityUpdates.Builder relationshipPropertyUpdate = gatherUpdatesFromCommandsForRelationship( relationshipId, relationshipCommand, propertyCommands );
        eagerlyGatherUpdates( relationshipPropertyUpdate, EntityType.RELATIONSHIP );
    }

    private void eagerlyGatherUpdates( EntityUpdates.Builder entityUpdatesBuilder, EntityType entityType )
    {
        EntityUpdates entityUpdates = entityUpdatesBuilder.build();
        Iterable<IndexDescriptor> relatedIndexes = schemaCache.getIndexesRelatedTo(
                entityUpdates.entityTokensChanged(),
                entityUpdates.entityTokensUnchanged(),
                entityUpdates.propertiesChanged(),
                entityUpdates.isPropertyListComplete(),
                entityType );
        // we need to materialize the IndexEntryUpdates here, because when we
        // consume (later in separate thread) the store might have changed.
        entityUpdates.forIndexKeys( relatedIndexes, reader, entityType ).forEach( updates::add );
    }

    private EntityUpdates.Builder gatherUpdatesFromCommandsForNode( long nodeId,
            NodeCommand nodeChanges,
            EntityCommandGrouper<NodeCommand>.Cursor propertyCommandsForNode )
    {
        long[] nodeLabelsBefore;
        long[] nodeLabelsAfter;
        if ( nodeChanges != null )
        {
            // Special case since the node may not be heavy, i.e. further loading may be required
            nodeLabelsBefore = parseLabelsField( nodeChanges.getBefore() ).get( nodeStore );
            nodeLabelsAfter = parseLabelsField( nodeChanges.getAfter() ).get( nodeStore );
        }
        else
        {
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
            StorageNodeCursor nodeCursor = loadNode( nodeId );
            nodeLabelsBefore = nodeLabelsAfter = nodeCursor.labels();
        }

        // First get possible Label changes
        boolean complete = providesCompleteListOfProperties( nodeChanges );
        EntityUpdates.Builder nodePropertyUpdates =
                EntityUpdates.forEntity( nodeId, complete ).withTokens( nodeLabelsBefore ).withTokensAfter( nodeLabelsAfter );

        // Then look for property changes
        converter.convertPropertyRecord( propertyCommandsForNode, nodePropertyUpdates );
        return nodePropertyUpdates;
    }

    private static boolean providesCompleteListOfProperties( Command entityCommand )
    {
        return entityCommand != null && (entityCommand.getMode() == CREATE || entityCommand.getMode() == DELETE);
    }

    private EntityUpdates.Builder gatherUpdatesFromCommandsForRelationship( long relationshipId, RelationshipCommand relationshipCommand,
            EntityCommandGrouper<RelationshipCommand>.Cursor propertyCommands )
    {
        long reltypeBefore;
        long reltypeAfter;
        if ( relationshipCommand != null )
        {
            reltypeBefore = relationshipCommand.getBefore().getType();
            reltypeAfter = relationshipCommand.getAfter().getType();
        }
        else
        {
            reltypeBefore = reltypeAfter = loadRelationship( relationshipId ).type();
        }
        boolean complete = providesCompleteListOfProperties( relationshipCommand );
        EntityUpdates.Builder relationshipPropertyUpdates =
                EntityUpdates.forEntity( relationshipId, complete ).withTokens( reltypeBefore ).withTokensAfter( reltypeAfter );
        converter.convertPropertyRecord( propertyCommands, relationshipPropertyUpdates );
        return relationshipPropertyUpdates;
    }

    private StorageNodeCursor loadNode( long nodeId )
    {
        if ( nodeCursor == null )
        {
            nodeCursor = reader.allocateNodeCursor();
        }
        nodeCursor.single( nodeId );
        if ( !nodeCursor.next() )
        {
            throw new IllegalStateException( "Node[" + nodeId + "] doesn't exist" );
        }
        return nodeCursor;
    }

    private StorageRelationshipScanCursor loadRelationship( long relationshipId )
    {
        if ( relationshipCursor == null )
        {
            relationshipCursor = reader.allocateRelationshipScanCursor();
        }
        relationshipCursor.single( relationshipId );
        if ( !relationshipCursor.next() )
        {
            throw new IllegalStateException( "Relationship[" + relationshipId + "] doesn't exist" );
        }
        return relationshipCursor;
    }

    @Override
    public void close()
    {
        closeAllUnchecked( nodeCursor, relationshipCursor, reader );
    }
}
