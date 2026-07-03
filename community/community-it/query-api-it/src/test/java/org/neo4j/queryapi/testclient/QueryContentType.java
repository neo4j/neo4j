package org.neo4j.queryapi.testclient;

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
public enum QueryContentType {
    UNTYPED("application/json", false, false),
    TYPED("application/vnd.neo4j.query", true, false),
    TYPED_V1_0("application/vnd.neo4j.query.v1.0", true, false),
    TYPED_V1_1("application/vnd.neo4j.query.v1.1", true, false),
    TYPED_V1_2("application/vnd.neo4j.query.v1.2", true, false),
    UNTYPED_L("application/jsonl", false, true),
    TYPED_L_V1_0("application/vnd.neo4j.query.v1.0+jsonl", true, true),
    TYPED_L_V1_1("application/vnd.neo4j.query.v1.1+jsonl", true, true),
    TYPED_L_V1_2("application/vnd.neo4j.query.v1.2+jsonl", true, true);

    private final String mimeType;
    private final boolean typed;
    private final boolean events;

    QueryContentType(String mimeType, boolean typed, boolean events) {
        this.mimeType = mimeType;
        this.typed = typed;
        this.events = events;
    }

    public String mimeType() {
        return mimeType;
    }

    public boolean typed() {
        return typed;
    }

    public boolean events() {
        return events;
    }
}
