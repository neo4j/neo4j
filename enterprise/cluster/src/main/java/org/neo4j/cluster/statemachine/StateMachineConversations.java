/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.statemachine;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.cluster.InstanceId;

/**
 * Generate id's for state machine conversations. This should be shared between all state machines in a server.
 * <p>
 * These conversation id's can be used to uniquely identify conversations between distributed state machines.
 */
public class StateMachineConversations
{
    private final AtomicLong nextConversationId = new AtomicLong();
    private final String serverId;

    public StateMachineConversations( InstanceId me )
    {
        serverId = me.toString();
    }

    public String getNextConversationId()
    {
        return serverId + "/" + nextConversationId.incrementAndGet() + "#";
    }
}
