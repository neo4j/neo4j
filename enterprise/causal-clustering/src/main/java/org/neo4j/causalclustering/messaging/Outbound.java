/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
