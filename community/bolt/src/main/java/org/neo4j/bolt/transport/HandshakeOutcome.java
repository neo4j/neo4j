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
package org.neo4j.bolt.transport;

public enum HandshakeOutcome
{
    /** Yay! */
    PROTOCOL_CHOSEN,
    /** Pending more bytes before handshake can complete */
    PARTIAL_HANDSHAKE,
    /** the client sent an invalid handshake */
    INVALID_HANDSHAKE,
    /** None of the clients suggested protocol versions are available :( */
    NO_APPLICABLE_PROTOCOL,
    /** Encryption is required but the connection is not encrypted */
    INSECURE_HANDSHAKE
}
