/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.schema;

/**
 * The type of index defined by this schema.
 * <p>
 * NOTE: The ordinal is used in the hash function for the auto-generated SchemaRule names, so avoid changing the ordinals when modifying this enum.
 */
public enum IndexType
{
    /**
     * For GBPTree based indexes. All types of values are indexed and stored in sort-order. This means they are good at all types of exact matching,
     * and range queries. They also support index-backed order-by.
     */
    BTREE,
    /**
     * For the fulltext schema indexes. These indexes do not index all value types, and cannot answer all types of queries.
     * On the other hand, they are good at CONTAINS and ENDS_WITH queries, and they can do fuzzy matching, and scoring.
     */
    FULLTEXT
}
