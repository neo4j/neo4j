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
package org.neo4j.kernel.impl.newapi;

import java.util.function.Predicate;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;

public class CursorPredicates {

    public static boolean propertiesMatch(PropertyCursor propertyCursor, PropertyIndexQuery[] queries) {
        int targetCount = queries.length;
        while (propertyCursor.next()) {
            for (PropertyIndexQuery query : queries) {
                if (propertyCursor.propertyKey() == query.propertyKeyId()) {
                    if (query.acceptsValueAt(propertyCursor)) {
                        targetCount--;
                        if (targetCount == 0) {
                            return true;
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
        return false;
    }

    public static Predicate<NodeCursor> hasLabel(int labelId) {
        return cursor -> cursor.hasLabel(labelId);
    }

    public static Predicate<RelationshipScanCursor> hasType(int typeId) {
        return cursor -> cursor.type() == typeId;
    }

    public static Predicate<NodeCursor> nodeMatchProperties(
            PropertyIndexQuery[] queries, PropertyCursor propertyCursor) {
        return cursor -> {
            cursor.properties(propertyCursor);
            return propertiesMatch(propertyCursor, queries);
        };
    }

    public static Predicate<RelationshipScanCursor> relationshipMatchProperties(
            PropertyIndexQuery[] queries, PropertyCursor propertyCursor) {
        return cursor -> {
            cursor.properties(propertyCursor);
            return propertiesMatch(propertyCursor, queries);
        };
    }
}
