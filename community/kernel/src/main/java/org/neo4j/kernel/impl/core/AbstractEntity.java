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
package org.neo4j.kernel.impl.core;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.indexOf;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.internal.kernel.api.EntityCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.storageengine.api.PropertySelection;

public abstract class AbstractEntity implements Entity {

    protected boolean hasProperty(String key, EntityCursor singleEntity, PropertyCursor properties) {
        if (null == key) {
            return false;
        }

        int propertyKey = tokenRead().propertyKey(key);
        if (propertyKey == TokenRead.NO_TOKEN) {
            return false;
        }

        singleEntity.properties(properties, PropertySelection.selection(propertyKey));
        return properties.next();
    }

    protected Object getProperty(String key, EntityCursor singleEntity, PropertyCursor properties) {
        if (null == key) {
            throw new IllegalArgumentException("(null) property key is not allowed");
        }
        int propertyKey = tokenRead().propertyKey(key);
        if (propertyKey == TokenRead.NO_TOKEN) {
            throw new NotFoundException(format("No such property, '%s'.", key));
        }

        singleEntity.properties(properties, PropertySelection.selection(propertyKey));
        if (!properties.next()) {
            throw new NotFoundException(format("No such property, '%s'.", key));
        }
        return properties.propertyValue().asObjectCopy();
    }

    protected Object getProperty(
            String key, Object defaultValue, EntityCursor singleEntity, PropertyCursor properties) {
        if (null == key) {
            throw new IllegalArgumentException("(null) property key is not allowed");
        }
        int propertyKey = tokenRead().propertyKey(key);
        if (propertyKey == TokenRead.NO_TOKEN) {
            return defaultValue;
        }
        singleEntity.properties(properties, PropertySelection.selection(propertyKey));
        return properties.next() ? properties.propertyValue().asObjectCopy() : defaultValue;
    }

    protected Iterable<String> getPropertyKeys(EntityCursor singleEntity, PropertyCursor properties) {
        List<String> keys = new ArrayList<>();
        try {
            TokenRead token = tokenRead();
            singleEntity.properties(properties, ALL_PROPERTIES);
            while (properties.next()) {
                keys.add(token.propertyKeyName(properties.propertyKey()));
            }
        } catch (PropertyKeyIdNotFoundKernelException e) {
            throw new IllegalStateException("Property key retrieved through kernel API should exist.", e);
        }
        return keys;
    }

    protected Map<String, Object> getProperties(EntityCursor singleEntity, PropertyCursor properties, String... keys) {
        Objects.requireNonNull(keys, "Properties keys should be not null array.");

        if (keys.length == 0) {
            return Collections.emptyMap();
        }

        int[] propertyIds = propertyIds(tokenRead(), keys);
        Map<String, Object> result = new HashMap<>(propertyIds.length);
        singleEntity.properties(properties, PropertySelection.selection(propertyIds));
        while (properties.next()) {
            result.put(
                    keys[indexOf(propertyIds, properties.propertyKey())],
                    properties.propertyValue().asObjectCopy());
        }
        return result;
    }

    public Map<String, Object> getAllProperties(EntityCursor singleEntity, PropertyCursor properties) {
        Map<String, Object> result = new HashMap<>();
        try {
            TokenRead token = tokenRead();
            singleEntity.properties(properties, ALL_PROPERTIES);
            while (properties.next()) {
                result.put(
                        token.propertyKeyName(properties.propertyKey()),
                        properties.propertyValue().asObjectCopy());
            }
        } catch (PropertyKeyIdNotFoundKernelException e) {
            throw new IllegalStateException("Property key retrieved through kernel API should exist.", e);
        }
        return result;
    }

    private int[] propertyIds(TokenRead tokenRead, String... keys) {
        // Find ids, note we are betting on that the number of keys
        // is small enough not to use a set here.
        int[] propertyIds = new int[keys.length];
        for (int i = 0; i < propertyIds.length; i++) {
            String key = keys[i];
            if (key == null) {
                throw new NullPointerException(String.format("Key %d was null", i));
            }
            propertyIds[i] = tokenRead.propertyKey(key);
        }

        return propertyIds;
    }

    protected abstract TokenRead tokenRead();
}
