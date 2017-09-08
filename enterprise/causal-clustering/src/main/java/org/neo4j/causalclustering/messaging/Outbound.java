/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

/**
 * A best effort service for delivery of messages to members. No guarantees are made about any of the methods
 * in terms of eventual delivery. The only non trivial promises is that no messages get duplicated and nothing gets
 * delivered to the wrong host.
 *
 * @param <MEMBER> The type of members that messages will be sent to.
 */
public interface Outbound<MEMBER, MESSAGE extends Message>
{
    /**
     * Asynchronous, best effort delivery to destination.
     *
     * @param to destination
     * @param message The message to send
     */
    default void send( MEMBER to, MESSAGE message )
    {
        send( to, message, false );
    }

    /**
     * Best effort delivery to destination.
     * <p>
     * Blocking waits at least until the I/O operation
     * completes, but it might still have failed.
     *
     * @param to destination
     * @param message the message to send
     * @param block whether to block until I/O completion
     */
    void send( MEMBER to, MESSAGE message, boolean block );
}
