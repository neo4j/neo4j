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
package org.neo4j.bolt.protocol.common.message.request.connection;

import java.util.List;
import java.util.Objects;
import org.neo4j.bolt.protocol.common.message.request.ImpersonationRequestMessage;
import org.neo4j.values.virtual.MapValue;

public final class RouteMessage implements ImpersonationRequestMessage {

    public static final byte SIGNATURE = 0x66;

    private final MapValue requestContext;
    private final List<String> bookmarks;
    private final String databaseName;
    private final String impersonatedUser;

    public RouteMessage(MapValue requestContext, List<String> bookmarks, String databaseName, String impersonatedUser) {
        this.requestContext = requestContext;
        this.bookmarks = bookmarks;
        this.databaseName = databaseName;
        this.impersonatedUser = impersonatedUser;
    }

    @Override
    public String impersonatedUser() {
        return impersonatedUser;
    }

    public MapValue getRequestContext() {
        return requestContext;
    }

    public List<String> getBookmarks() {
        return bookmarks;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RouteMessage that = (RouteMessage) o;
        return Objects.equals(requestContext, that.requestContext)
                && Objects.equals(bookmarks, that.bookmarks)
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(impersonatedUser, that.impersonatedUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestContext, bookmarks, databaseName, impersonatedUser);
    }

    @Override
    public String toString() {
        return "RouteMessage{" + "requestContext="
                + requestContext + ", bookmarks="
                + bookmarks + ", databaseName='"
                + databaseName + '\'' + ", impersonatedUser='"
                + impersonatedUser + '\'' + '}';
    }
}
