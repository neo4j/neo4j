/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.messaging;

import java.util.Collection;

/**
 * A best effort service for delivery of messages to members. No guarantees are made about any of the methods
 * in terms of eventual delivery. The only non trivial promises is that no messages get duplicated and nothing gets
 * delivered to the wrong host.
 * @param <MEMBER> The type of members that messages will be sent to.
 */
public interface Outbound<MEMBER, MESSAGE extends Message>
{
    /**
     * Asynchronous, best effort delivery to destination.
     * @param to destination
     * @param message The message to send
     */
    void send( MEMBER to, MESSAGE message );

    /**
     * Asynchronous, best effort delivery to destination.
     * @param to destination
     * @param messages The messages to send
     */
    void send( MEMBER to, Collection<MESSAGE> messages );
}
