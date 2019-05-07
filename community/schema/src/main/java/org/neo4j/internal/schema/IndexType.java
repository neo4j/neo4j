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
 */
public enum IndexType
{
    /**
     * Used for indicating an arbitrary index type of kind {@link IndexKind#GENERAL}. This is used for when the concrete index type is not so important,
     * as long as the index kind is GENERAL. For instance, when creating indexes an no type is explicitly asked for, or when querying for available indexes.
     * <p>
     * We might be able to remove this once indexes are referenced by name, rather than by schema.
     */
    ANY_GENERAL( IndexKind.GENERAL ),
    /**
     * For GBPTree based indexes. All types of values are indexed and stored in sort-order. This means they are good at all types of exact matching,
     * and range queries. They also support index-backed order-by.
     */
    TREE( IndexKind.GENERAL ),
    /**
     * For the fulltext schema indexes. These indexes do not index all value types, and cannot answer all types of queries.
     * On the other hand, they are good at CONTAINS and ENDS_WITH queries, and they can do fuzzy matching, and scoring.
     */
    FULLTEXT( IndexKind.SPECIAL ),
    /**
     * For Lucene based indexes. Lucene is an inverted-index implementation. These indexes are good at exact matching, and can also do CONTAINS and
     * ENDS_WITH queries on string values, but they cannot do fuzzy matching, and they also do not support index-backed order-by.
     */
    INVERTED( IndexKind.GENERAL ),
    ;

    private final IndexKind kind;

    IndexType( IndexKind kind )
    {
        this.kind = kind;
    }

    public IndexKind getKind()
    {
        return kind;
    }
}
