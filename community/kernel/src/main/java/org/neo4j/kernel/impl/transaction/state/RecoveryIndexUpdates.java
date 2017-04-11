/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;

/**
 * Used during recovery to collect node ids to refresh from scratch after recovery is completed.
 * The reason for this is that index updates from transaction commands may need to read from store
 * and the store needs to be in a recovered state before reading from it.
 */
public class RecoveryIndexUpdates implements IndexUpdates
{
    private final PrimitiveLongSet ids = Primitive.longSet();

    @Override
    public Iterator<IndexEntryUpdate<LabelSchemaDescriptor>> iterator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void collectUpdatedNodeIds( PrimitiveLongSet target )
    {
        target.addAll( ids.iterator() );
    }

    @Override
    public void feed( PrimitiveLongObjectMap<List<PropertyCommand>> propCommands,
            PrimitiveLongObjectMap<NodeCommand> nodeCommands )
    {
        ids.addAll( propCommands.iterator() );
        ids.addAll( nodeCommands.iterator() );
    }

    @Override
    public boolean hasUpdates()
    {
        return !ids.isEmpty();
    }
}
