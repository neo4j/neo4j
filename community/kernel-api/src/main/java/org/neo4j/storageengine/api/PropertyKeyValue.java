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
package org.neo4j.storageengine.api;

import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Default implementation of {@link StorageProperty} where the {@link Value} has already been materialized.
 */
public record PropertyKeyValue(int propertyKeyId, Value value) implements StorageProperty {
    public PropertyKeyValue {
        assert value != null;
    }

    @Override
    public boolean isDefined() {
        return value != Values.NO_VALUE;
    }

    @Override
    public String toString() {
        return "Property{" + propertyKeyId + "=" + value + '}';
    }

    @Override
    public String toString(TokenHolder tokenHolder) {
        String propertyKeyName;
        try {
            propertyKeyName = "'" + tokenHolder.getTokenById(propertyKeyId).name() + "'";
        } catch (TokenNotFoundException e) {
            propertyKeyName = String.valueOf(propertyKeyId);
        }
        return "Property{" + propertyKeyName + "=" + value + '}';
    }
}
