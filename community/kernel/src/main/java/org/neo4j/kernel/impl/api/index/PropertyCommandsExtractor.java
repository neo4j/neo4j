/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.storageengine.api.CommandsToApply;

import static org.neo4j.kernel.impl.store.NodeLabelsField.fieldPointsToDynamicRecordOfLabels;

/**
 * Implements both BatchTransactionApplier and TransactionApplier in order to reduce garbage.
 * Gathers node/property commands by node id, preparing for extraction of {@link EntityUpdates updates}.
 */
public class PropertyCommandsExtractor extends TransactionApplier.Adapter
        implements BatchTransactionApplier
{
    private final MutableLongObjectMap<NodeCommand> nodeCommandsById = longObjectMap();
    private final MutableLongObjectMap<RelationshipCommand> relationshipCommandsById = longObjectMap();
    private final MutableLongObjectMap<List<PropertyCommand>> propertyCommandsByNodeIds = longObjectMap();
    private final MutableLongObjectMap<List<PropertyCommand>> propertyCommandsByRelationshipIds = longObjectMap();
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
    public boolean visitRelationshipCommand( RelationshipCommand command ) throws IOException
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
            long nodeId = command.getAfter().getNodeId();
            List<PropertyCommand> group = propertyCommandsByNodeIds.get( nodeId );
            if ( group == null )
            {
                propertyCommandsByNodeIds.put( nodeId, group = new ArrayList<>() );
            }
            group.add( command );
            hasUpdates = true;
        }
        else if ( command.getAfter().isRelSet() )
        {
            long relId = command.getAfter().getRelId();
            List<PropertyCommand> group = propertyCommandsByRelationshipIds.get( relId );
            if ( group == null )
            {
                propertyCommandsByRelationshipIds.put( relId, group = new ArrayList<>() );
            }
            group.add( command );
            hasUpdates = true;
        }
        return false;
    }

    public boolean containsAnyEntityOrPropertyUpdate()
    {
        return hasUpdates;
    }

    public LongObjectMap<NodeCommand> nodeCommandsById()
    {
        return nodeCommandsById;
    }

    public PrimitiveLongObjectMap<RelationshipCommand> relationshipCommandsById()
    {
        return relationshipCommandsById;
    }

    public LongObjectMap<List<PropertyCommand>> propertyCommandsByNodeIds()
    {
        return propertyCommandsByNodeIds;
    }
    public PrimitiveLongObjectMap<List<PropertyCommand>> propertyCommandsByRelationshipIds()
    {
        return propertyCommandsByRelationshipIds;
    }
}
