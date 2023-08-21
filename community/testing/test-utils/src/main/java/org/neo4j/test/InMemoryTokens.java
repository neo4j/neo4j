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
package org.neo4j.test;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.common.TokenNameLookup;

public class InMemoryTokens implements TokenNameLookup {
    private final Map<Integer, String> labels = new HashMap<>();
    private final Map<Integer, String> relationshipTypes = new HashMap<>();
    private final Map<Integer, String> propertyKeys = new HashMap<>();

    @Override
    public String labelGetName(int labelId) {
        return labels.getOrDefault(labelId, "[" + labelId + "]");
    }

    @Override
    public String relationshipTypeGetName(int relationshipTypeId) {
        return relationshipTypes.getOrDefault(relationshipTypeId, "[" + relationshipTypeId + "]");
    }

    @Override
    public String propertyKeyGetName(int propertyKeyId) {
        return propertyKeys.getOrDefault(propertyKeyId, "[" + propertyKeyId + "]");
    }

    public InMemoryTokens label(int id, String name) {
        labels.put(id, name);
        return this;
    }

    public InMemoryTokens relationshipType(int id, String name) {
        relationshipTypes.put(id, name);
        return this;
    }

    public InMemoryTokens propertyKey(int id, String name) {
        propertyKeys.put(id, name);
        return this;
    }
}
