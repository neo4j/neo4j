/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.query.clientconnection;

import static org.neo4j.configuration.helpers.SocketAddress.format;

import java.net.SocketAddress;
import java.util.Map;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;

/**
 * @see ClientConnectionInfo Parent class for documentation and tests.
 */
public class BoltConnectionInfo extends ClientConnectionInfo {
    private final String connectionId;
    private final String clientName;
    private final SocketAddress clientAddress;
    private final SocketAddress serverAddress;
    private final Map<String, String> boltAgent;

    public BoltConnectionInfo(
            String connectionId,
            String clientName,
            SocketAddress clientAddress,
            SocketAddress serverAddress,
            Map<String, String> boltAgent) {
        this.connectionId = connectionId;
        this.clientName = clientName;
        this.clientAddress = clientAddress;
        this.serverAddress = serverAddress;
        this.boltAgent = boltAgent;
    }

    @Override
    public String asConnectionDetails() {
        return "bolt-session\tbolt\t" + clientName + "\t\tclient" + clientAddress + "\tserver" + serverAddress + ">";
    }

    @Override
    public String protocol() {
        return "bolt";
    }

    @Override
    public String connectionId() {
        return connectionId;
    }

    @Override
    public String clientAddress() {
        return format(clientAddress);
    }

    @Override
    public String requestURI() {
        return format(serverAddress);
    }

    @Override
    public Map<String, String> boltAgent() {
        return boltAgent;
    }
}
