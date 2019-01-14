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
package org.neo4j.bolt.transport;

import org.neo4j.bolt.BoltChannel;

/**
 * Represents a component that instantiates Bolt protocol handlers.
 *
 * @see BoltProtocolPipelineInstaller
 */
@FunctionalInterface
public interface BoltProtocolPipelineInstallerFactory
{
    /**
     * Instantiate a handler for Bolt protocol with the specified version. Return {@code null} when handler for the
     * given version can't be instantiated.
     *
     * @param protocolVersion the version as negotiated by the initial handshake.
     * @param channel the channel representing network connection from the client.
     * @return new protocol handler when given protocol version is known and valid, {@code null} otherwise.
     */
    BoltProtocolPipelineInstaller create( long protocolVersion, BoltChannel channel );
}
