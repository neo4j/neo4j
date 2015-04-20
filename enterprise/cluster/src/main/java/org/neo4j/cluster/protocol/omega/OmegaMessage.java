/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.protocol.omega;

import org.neo4j.cluster.com.message.MessageType;

public enum OmegaMessage implements MessageType
{
    /*
     * Boring administrative messages
     */
    add_listener, remove_listener, start,
    /*
     * The required timeouts
     */
    refresh_timeout, round_trip_timeout, read_timeout,
    /*
     * The refresh request, where we send our state to
     * f other processes
     */
    refresh,
    /*
     * The message to respond with on refresh requests
     */
    refresh_ack,
    /*
     * The collect request, sent to gather up the states of
     * n-f other machines
     */
    collect,
    /*
     * The response to a collect request, sending
     * back the registry
     */
    status;
}
