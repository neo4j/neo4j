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
package org.neo4j.bolt.v3.messaging.request;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltStateMachine;

/**
 * On decoding of a {@link GoodbyeMessage}, we immediately stop whatever the connection is doing and shut down this connection.
 * As the {@link BoltStateMachine} with this connection will also shut down at the same time,
 * this message will actually NEVER be handled by {@link BoltStateMachine}.
 */
public class GoodbyeMessage implements RequestMessage
{
    public static final byte SIGNATURE = 0x02;
    public static final GoodbyeMessage GOODBYE_MESSAGE = new GoodbyeMessage();

    private GoodbyeMessage()
    {
        // left empty on purpose
    }

    @Override
    public boolean safeToProcessInAnyState()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return "GOODBYE";
    }
}
