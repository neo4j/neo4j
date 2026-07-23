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
package org.neo4j.server.queryapi.request.plainjson;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.server.queryapi.request.QueryRequestCypherValue;

/**
 * Describes the shape of the value on {@link org.neo4j.server.queryapi.QueryMimeTypes#PLAIN_JSON}
 */
public class PlainJsonQueryRequestCypherValue implements QueryRequestCypherValue {
    private final Value value;

    private PlainJsonQueryRequestCypherValue(Value value) {
        this.value = value;
    }

    protected static PlainJsonQueryRequestCypherValue ofObject(Object object) {
        return new PlainJsonQueryRequestCypherValue(Values.value(object));
    }

    protected static PlainJsonQueryRequestCypherValue ofValue(Value value) {
        return new PlainJsonQueryRequestCypherValue(value);
    }

    @Override
    public Value value() {
        return value;
    }
}
