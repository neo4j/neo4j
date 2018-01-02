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

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Implemented by states in a state machine. Each state must
 * implement the handle method, to perform different things depending
 * on the message that comes in. This should only be implemented as enums.
 * <p>
 * A state is guaranteed to only have one handle at a time, i.e. access is serialized.
 */
public interface State<CONTEXT, MESSAGETYPE extends MessageType>
{
    /**
     * Handle a message. The state can use context for state storage/retrieval and it will also act
     * as a facade to the rest of the system. The MessageProcessor is used to trigger new messages.
     * When the handling is done the state returns the next state of the state machine.
     *
     *
     * @param context  that contains state and methods to access other parts of the system
     * @param message  that needs to be handled
     * @param outgoing processor for new messages created by the handling of this message
     * @return the new state
     * @throws Throwable
     */
    public State<?, ?> handle( CONTEXT context, Message<MESSAGETYPE> message, MessageHolder outgoing ) throws
            Throwable;
}
