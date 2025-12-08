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
package org.neo4j.server.queryapi;

import javax.ws.rs.core.MediaType;

/**
 * Holds methods related to Mime Types used in the Query API.
 */
public final class QueryMimeTypes {
    public static final String UNTYPED_JSON = MediaType.APPLICATION_JSON;
    public static final String TYPED_JSON = "application/vnd.neo4j.query";
    public static final String TYPED_JSON_V1x0 = "application/vnd.neo4j.query.v1.0";
    public static final String ALL = MediaType.APPLICATION_JSON + "," + TYPED_JSON + "," + TYPED_JSON_V1x0;

    private QueryMimeTypes() {}

    public static boolean hasTyped(String contentType) {
        return TYPED_JSON_V1x0.equals(contentType) || TYPED_JSON.equals(contentType);
    }

    public static boolean hasUntyped(String contentType) {
        return UNTYPED_JSON.equals(contentType);
    }
}
