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
package org.neo4j.fabric.bolt;

import java.util.List;
import java.util.UUID;
import org.neo4j.fabric.bookmark.RemoteBookmark;

public record QueryRouterBookmark(
        List<InternalGraphState> internalGraphStates, List<ExternalGraphState> externalGraphStates) {

    /**
     * State of a graph that is located in current DBMS.
     */
    public record InternalGraphState(UUID graphUuid, long transactionId) {}

    /**
     * State of a graph that is located in another DBMS.
     */
    public record ExternalGraphState(UUID graphUuid, List<RemoteBookmark> bookmarks) {}
}
