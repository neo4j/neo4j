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
package org.neo4j.kernel.impl.api.index;

import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.storageengine.api.CommandsToApply;

import static org.neo4j.kernel.impl.store.NodeLabelsField.fieldPointsToDynamicRecordOfLabels;

/**
 * Implements both BatchTransactionApplier and TransactionApplier in order to reduce garbage.
 * Gathers node/property commands by node id, preparing for extraction of {@link EntityUpdates updates}.
 */
public class PropertyCommandsExtractor extends TransactionApplier.Adapter
        implements BatchTransactionApplier
{
    private final MutableLongObjectMap<NodeCommand> nodeCommandsById = new LongObjectHashMap<>();
    private final MutableLongObjectMap<Command.RelationshipCommand> relationshipCommandsById = new LongObjectHashMap<>();
    private final MutableLongObjectMap<List<PropertyCommand>> propertyCommandsByNodeIds = new LongObjectHashMap<>();
    private final MutableLongObjectMap<List<PropertyCommand>> propertyCommandsByRelationshipIds = new LongObjectHashMap<>();
    private boolean hasUpdates;

    @Override
    public TransactionApplier startTx( CommandsToApply transaction )
    {
        return this;
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction, LockGroup lockGroup )
    {
        return startTx( transaction );
    }

    @Override
    public void close()
    {
        nodeCommandsById.clear();
        relationshipCommandsById.clear();
        propertyCommandsByNodeIds.clear();
        propertyCommandsByRelationshipIds.clear();
    }

    @Override
    public boolean visitNodeCommand( NodeCommand command )
    {
        nodeCommandsById.put( command.getKey(), command );
        if ( !hasUpdates && mayResultInIndexUpdates( command ) )
        {
            hasUpdates = true;
        }
        return false;
    }

    @Override
    public boolean visitRelationshipCommand( Command.RelationshipCommand command )
    {
        relationshipCommandsById.put( command.getKey(), command );
        hasUpdates = true;
        return false;
    }

    private static boolean mayResultInIndexUpdates( NodeCommand command )
    {
        long before = command.getBefore().getLabelField();
        long after = command.getAfter().getLabelField();
        return before != after ||
                // Because we don't know here, there may have been changes to a dynamic label record
                // even though it still points to the same one
                fieldPointsToDynamicRecordOfLabels( before ) || fieldPointsToDynamicRecordOfLabels( after );
    }

    @Override
    public boolean visitPropertyCommand( PropertyCommand command )
    {
        if ( command.getAfter().isNodeSet() )
        {
            createOrAddToGroup( command, command.getAfter().getNodeId(), propertyCommandsByNodeIds );
        }
        else if ( command.getAfter().isRelSet() )
        {
            createOrAddToGroup( command, command.getAfter().getRelId(), propertyCommandsByRelationshipIds );
        }
        return false;
    }

    private void createOrAddToGroup( PropertyCommand command, long entityId, MutableLongObjectMap<List<PropertyCommand>> propertyCommandsByEntityIds )
    {
        List<PropertyCommand> group = propertyCommandsByEntityIds.get( entityId );
        if ( group == null )
        {
            propertyCommandsByEntityIds.put( entityId, group = new ArrayList<>() );
        }
        group.add( command );
        hasUpdates = true;
    }

    public boolean containsAnyEntityOrPropertyUpdate()
    {
        return hasUpdates;
    }

    public LongObjectMap<NodeCommand> nodeCommandsById()
    {
        return nodeCommandsById;
    }

    public LongObjectMap<Command.RelationshipCommand> relationshipCommandsById()
    {
        return relationshipCommandsById;
    }

    public LongObjectMap<List<PropertyCommand>> propertyCommandsByNodeIds()
    {
        return propertyCommandsByNodeIds;
    }

    public LongObjectMap<List<PropertyCommand>> propertyCommandsByRelationshipIds()
    {
        return propertyCommandsByRelationshipIds;
    }
}
