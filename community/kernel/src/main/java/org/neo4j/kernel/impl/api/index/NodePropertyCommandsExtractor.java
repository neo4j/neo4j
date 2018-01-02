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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;

import static org.neo4j.collection.primitive.Primitive.longObjectMap;
import static org.neo4j.collection.primitive.Primitive.longSet;

class NodePropertyCommandsExtractor
        extends CommandHandler.Adapter implements Visitor<Command,IOException>
{
    final PrimitiveLongObjectMap<NodeCommand> nodeCommandsById = longObjectMap();
    final PrimitiveLongObjectMap<List<PropertyCommand>> propertyCommandsByNodeIds = longObjectMap();

    @Override
    public boolean visit( Command element ) throws IOException
    {
        element.handle( this );
        return false;
    }

    public void clear()
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

    public void visitUpdatedNodeIds( PrimitiveLongVisitor<RuntimeException> updatedNodeVisitor )
    {
        try ( PrimitiveLongSet uniqueIds = longSet( nodeCommandsById.size() + propertyCommandsByNodeIds.size() ) )
        {
            uniqueIds.addAll( nodeCommandsById.iterator() );
            uniqueIds.addAll( propertyCommandsByNodeIds.iterator() );
            uniqueIds.visitKeys( updatedNodeVisitor );
        }
    }
}