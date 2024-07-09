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
package org.neo4j.internal.schema;

/**
 * This enum signifies how this schema should behave in regard to updates.
 * {@link SchemaPatternMatchingType#COMPLETE_ALL_TOKENS} this schema should match on ALL properties for the entities (node/relationships) with the specified single entity token (label/ relationship type)
 * i.e. when all properties are present: (prop1 && prop2 && ... && propN)
 * <p>
 * {@link SchemaPatternMatchingType#PARTIAL_ANY_TOKEN} this schema should match on ANY properties for the entities (node/relationships) with the specified single entity token (label/ relationship type)
 *  i.e. at least one of the properties is present: (prop1 || prop2 || ... || propN)
 * <p>
 * {@link SchemaPatternMatchingType#ENTITY_TOKENS} this schema should match on ANY/ALL entities of this schema {@link org.neo4j.common.EntityType}.
 * i.e. for a schema with EntityType.NODE it will match all nodes or any index / constraint that operates on nodes
 * <p>
 * {@link SchemaPatternMatchingType#SINGLE_ENTITY_TOKEN} this schema holds a single entity token and no property tokens, and should match on its presence
 * example: in relationship endpoint constraints this holds the id of the relationship type of the relationships we want to constraint
 * <p>
 * NOTE: The ordinal is used in the hash function for the auto-generated SchemaRule names, so avoid changing the ordinals when modifying this enum.
 */
public enum SchemaPatternMatchingType {
    COMPLETE_ALL_TOKENS,
    PARTIAL_ANY_TOKEN,
    ENTITY_TOKENS,
    SINGLE_ENTITY_TOKEN
}
