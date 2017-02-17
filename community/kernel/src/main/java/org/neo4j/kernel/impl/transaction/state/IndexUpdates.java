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

import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.index.NodeUpdates;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;

/**
 * Set of updates ({@link NodeUpdates}) to apply to indexes.
 */
public interface IndexUpdates extends Iterable<NodeUpdates>
{
    /**
     * Feeds updates raw material in the form of node/property commands, to create updates from.
     *
     * @param propCommands {@link PropertyCommand} grouped by node id.
     * @param nodeCommands {@link NodeCommand} by node id.
     */
    void feed( PrimitiveLongObjectMap<List<PropertyCommand>> propCommands,
            PrimitiveLongObjectMap<NodeCommand> nodeCommands );

    /**
     * Exposed since we infer index updates from physical commands AND contents in store.
     * This means that we cannot get to this information during recovery and so we merely need a way
     * to jot down which nodes needs to be re-indexed after recovery.
     *
     * @param target to receive these node ids.
     */
    void collectUpdatedNodeIds( PrimitiveLongSet target );

    boolean hasUpdates();
}
