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
package org.neo4j.configuration.helpers;

import java.util.List;
import java.util.Objects;

public class RemoteUri {
    private final String scheme;
    private final List<SocketAddress> addresses;
    private final String query;

    public RemoteUri(String scheme, List<SocketAddress> addresses, String query) {
        this.scheme = scheme;
        this.addresses = addresses;
        this.query = query;
    }

    public String getScheme() {
        return scheme;
    }

    public List<SocketAddress> getAddresses() {
        return addresses;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteUri remoteUri = (RemoteUri) o;
        return Objects.equals(scheme, remoteUri.scheme)
                && Objects.equals(addresses, remoteUri.addresses)
                && Objects.equals(query, remoteUri.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scheme, addresses, query);
    }

    @Override
    public String toString() {
        return "RemoteUri{" + "scheme='" + scheme + '\'' + ", addresses=" + addresses + ", query='" + query + '\''
                + '}';
    }
}
