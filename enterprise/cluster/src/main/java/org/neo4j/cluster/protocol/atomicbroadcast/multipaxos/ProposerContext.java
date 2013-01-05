/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Context used by {@link ProposerState} state machine.
 */
public class ProposerContext
{
    // Proposer/coordinator state
    Deque<Object> pendingValues = new LinkedList<Object>();
    Map<InstanceId, Object> bookedInstances = new HashMap<InstanceId, Object>();

    public long nextInstanceId = 0;

    public InstanceId newInstanceId( long lastLearnedInstanceId )
    {
        // Never propose something lower than last received instance id
        if ( lastLearnedInstanceId >= nextInstanceId )
        {
            nextInstanceId = lastLearnedInstanceId + 1;
        }

        return new InstanceId( nextInstanceId++ );
    }

    public void leave()
    {
        pendingValues.clear();
        bookedInstances.clear();
        nextInstanceId = 0;
    }
}
