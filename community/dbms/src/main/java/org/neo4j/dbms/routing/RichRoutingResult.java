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
package org.neo4j.dbms.routing;

import java.util.List;
import org.neo4j.configuration.helpers.SocketAddress;

/**
 * Extends {@link RoutingResult} with information needed by server-side routing
 */
public class RichRoutingResult extends RoutingResult {

    private final List<RichRoutingServerAddress> routeServerAddresses;
    private final List<RichRoutingServerAddress> writeServerAddresses;
    private final List<RichRoutingServerAddress> readServerAddresses;

    public RichRoutingResult(
            List<RichRoutingServerAddress> routeServerAddresses,
            List<RichRoutingServerAddress> writeServerAddresses,
            List<RichRoutingServerAddress> readServerAddresses,
            long timeToLiveMillis) {
        super(
                externalBoltAddresses(routeServerAddresses),
                externalBoltAddresses(writeServerAddresses),
                externalBoltAddresses(readServerAddresses),
                timeToLiveMillis);
        this.routeServerAddresses = routeServerAddresses;
        this.writeServerAddresses = writeServerAddresses;
        this.readServerAddresses = readServerAddresses;
    }

    public List<RichRoutingServerAddress> routeServerAddresses() {
        return routeServerAddresses;
    }

    public List<RichRoutingServerAddress> writeServerAddresses() {
        return writeServerAddresses;
    }

    public List<RichRoutingServerAddress> readServerAddresses() {
        return readServerAddresses;
    }

    private static List<SocketAddress> externalBoltAddresses(List<RichRoutingServerAddress> serverAddresses) {
        return serverAddresses.stream()
                .flatMap(sa -> sa.externalBoltAddress().stream())
                .toList();
    }
}
