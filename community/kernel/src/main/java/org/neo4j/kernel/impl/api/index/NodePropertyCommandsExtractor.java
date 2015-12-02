/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;

import static org.neo4j.collection.primitive.Primitive.longObjectMap;

/**
 * Implements both BatchTransactionApplier and TransactionApplier in order to reduce garbage.
 * Gathers node/property commands by node id, preparing for extraction of {@link NodePropertyUpdate updates}.
 */
public class NodePropertyCommandsExtractor extends TransactionApplier.Adapter
        implements BatchTransactionApplier
{
    private final PrimitiveLongObjectMap<NodeCommand> nodeCommandsById = longObjectMap();
    private final PrimitiveLongObjectMap<List<PropertyCommand>> propertyCommandsByNodeIds = longObjectMap();

    @Override
    public TransactionApplier startTx( TransactionToApply transaction )
    {
        return this;
    }

    @Override
    public TransactionApplier startTx( TransactionToApply transaction, LockGroup lockGroup )
    {
        return startTx( transaction );
    }

    @Override
    public void close() throws Exception
    {
        nodeCommandsById.clear();
        propertyCommandsByNodeIds.clear();
    }

    @Override
    public boolean visitNodeCommand( NodeCommand command ) throws IOException
    {
        nodeCommandsById.put( command.getKey(), command );
        return false;
    }

    @Override
    public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
    {
        PropertyRecord record = command.getAfter();
        if ( record.isNodeSet() )
        {
            long nodeId = command.getAfter().getNodeId();
            List<PropertyCommand> group = propertyCommandsByNodeIds.get( nodeId );
            if ( group == null )
            {
                propertyCommandsByNodeIds.put( nodeId, group = new ArrayList<>() );
            }
            group.add( command );
        }
        return false;
    }

    public boolean containsAnyNodeOrPropertyUpdate()
    {
        return !nodeCommandsById.isEmpty() || !propertyCommandsByNodeIds.isEmpty();
    }

    public PrimitiveLongObjectMap<NodeCommand> nodeCommandsById()
    {
        return nodeCommandsById;
    }

    public PrimitiveLongObjectMap<List<PropertyCommand>> propertyCommandsByNodeIds()
    {
        return propertyCommandsByNodeIds;
    }
}
