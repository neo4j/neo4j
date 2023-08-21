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

import static org.neo4j.values.storable.Values.NO_VALUE;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

/**
 * Cursor that can read property data.
 */
public interface StoragePropertyCursor extends StorageCursor {
    /**
     * Initializes this cursor to that reading node properties at the given {@code reference}.
     * @param reference reference to start reading node properties at.
     */
    void initNodeProperties(Reference reference, PropertySelection selection, long ownerReference);

    default void initNodeProperties(Reference reference, PropertySelection selection) {
        initNodeProperties(reference, selection, -1);
    }

    /**
     * Initializes this cursor to that reading node properties at the given {@code nodeCursor}.
     * @param nodeCursor {@link StorageNodeCursor} to start reading node properties at.
     */
    default void initNodeProperties(StorageNodeCursor nodeCursor, PropertySelection selection) {
        initNodeProperties(nodeCursor.propertiesReference(), selection);
    }

    /**
     * Initializes this cursor to that reading relationship properties at the given {@code reference}.
     * @param reference reference to start reading relationship properties at.
     */
    void initRelationshipProperties(Reference reference, PropertySelection selection, long ownerReference);

    default void initRelationshipProperties(Reference reference, PropertySelection selection) {
        initRelationshipProperties(reference, selection, -1);
    }

    /**
     * Initializes this cursor to that reading node properties at the given {@code relationshipCursor}.
     * @param relationshipCursor {@link StorageRelationshipCursor} to start reading relationship properties at.
     */
    default void initRelationshipProperties(StorageRelationshipCursor relationshipCursor, PropertySelection selection) {
        initRelationshipProperties(relationshipCursor.propertiesReference(), selection);
    }

    /**
     * @return property key of the property this cursor currently is placed at.
     */
    int propertyKey();

    /**
     * @return value group of the property this cursor currently is placed at.
     */
    ValueGroup propertyType();

    /**
     * @return value of the property this cursor currently is placed at.
     */
    Value propertyValue();

    /**
     * Seeks the given property key id and returns its value. This is a one-shot call and to get more properties from this
     * cursor it will have to be initialized again.
     *
     * @param propertyKeyId the property key id to get the value for.
     * @return the value for the given property key, or {@link Values#NO_VALUE} if no such property was found.
     * @deprecated only a temporary method to allow compiled runtime to continue working w/o bigger rewrite.
     */
    default Value seekPropertyValue(int propertyKeyId) {
        return seekProperty(propertyKeyId) ? propertyValue() : NO_VALUE;
    }

    /**
     * Seeks the given property key id and returns whether or not it exists. This is a one-shot call and to get more properties from this
     * cursor it will have to be initialized again.
     *
     * @param propertyKeyId the property key id to get the value for.
     * @return {@code true} if the property exists, otherwise {@code false}.
     * @deprecated only a temporary method to allow compiled runtime to continue working w/o bigger rewrite.
     */
    default boolean seekProperty(int propertyKeyId) {
        while (next()) {
            if (propertyKeyId == propertyKey()) {
                return true;
            }
        }
        return false;
    }
}
