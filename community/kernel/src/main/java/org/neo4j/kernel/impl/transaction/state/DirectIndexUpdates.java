/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;

/**
 * Provides direct access to updates.
 */
public class DirectIndexUpdates implements IndexUpdates
{
    private final Iterable<NodePropertyUpdate> updates;

    public DirectIndexUpdates( Iterable<NodePropertyUpdate> updates )
    {
        this.updates = updates;
    }

    @Override
    public Iterator<NodePropertyUpdate> iterator()
    {
        return updates.iterator();
    }

    @Override
    public void collectUpdatedNodeIds( PrimitiveLongSet target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void feed( PrimitiveLongObjectMap<List<PropertyCommand>> propCommands,
            PrimitiveLongObjectMap<NodeCommand> nodeCommands )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasUpdates()
    {
        return true;
    }
}
