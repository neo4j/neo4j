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
package org.neo4j.proxy;

import java.net.InetSocketAddress;
import org.neo4j.test.ports.PortAuthority;

public record ProxyConfiguration(InetSocketAddress listenAddress, InetSocketAddress advertisedAddress) {
    /**
     * Construct listenAddress and advertisedAddress by allocating ports
     * */
    public static ProxyConfiguration buildProxyConfig() {
        return new ProxyConfiguration(
                new InetSocketAddress("localhost", PortAuthority.allocatePort()),
                new InetSocketAddress("localhost", PortAuthority.allocatePort()));
    }

    public String advertisedAddressToStr() {
        return advertisedAddress.getHostName() + ":" + advertisedAddress.getPort();
    }

    public String listenAddressToStr() {
        return listenAddress.getHostName() + ":" + listenAddress.getPort();
    }
}
