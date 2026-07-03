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
    // JSON
    public static final String PLAIN_JSON = MediaType.APPLICATION_JSON;
    public static final String TYPED_JSON = "application/vnd.neo4j.query";
    public static final String TYPED_JSON_V1x0 = "application/vnd.neo4j.query.v1.0";
    public static final String TYPED_JSON_V1x1 = "application/vnd.neo4j.query.v1.1";
    public static final String TYPED_JSON_V1x2 = "application/vnd.neo4j.query.v1.2";
    // JSON LINES
    public static final String PLAIN_JSONL = "application/jsonl";
    public static final String TYPED_JSONL_V1x0 = TYPED_JSON_V1x0 + "+jsonl";
    public static final String TYPED_JSONL_V1x1 = TYPED_JSON_V1x1 + "+jsonl";
    public static final String TYPED_JSONL_V1x2 = TYPED_JSON_V1x2 + "+jsonl";
    // AGGREGATES
    public static final String ALL_JSON = MediaType.APPLICATION_JSON + "," + TYPED_JSON + "," + TYPED_JSON_V1x0 + ","
            + TYPED_JSON_V1x1 + "," + TYPED_JSON_V1x2;
    public static final String ALL_JSONL =
            PLAIN_JSONL + "," + TYPED_JSONL_V1x0 + "," + TYPED_JSONL_V1x1 + "," + TYPED_JSONL_V1x2;

    private QueryMimeTypes() {}

    public static boolean hasTyped(String contentType) {
        return TYPED_JSON_V1x2.equals(contentType)
                || TYPED_JSON_V1x1.equals(contentType)
                || TYPED_JSON_V1x0.equals(contentType)
                || TYPED_JSON.equals(contentType);
    }

    public static boolean hasTypedJsonl(String contentType) {
        return TYPED_JSONL_V1x0.equals(contentType)
                || TYPED_JSONL_V1x1.equals(contentType)
                || TYPED_JSONL_V1x2.equals(contentType);
    }

    public static boolean hasUntyped(String contentType) {
        return PLAIN_JSON.equals(contentType);
    }

    public static boolean hasUntypedJsonl(String contentType) {
        return PLAIN_JSONL.equals(contentType);
    }
}
