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
 * This is the internal equivalent of {@link org.neo4j.graphdb.schema.IndexType}.
 * <p>
 * NOTE: The typeNumber is used in the hash function for the auto-generated SchemaRule names, so avoid changing them when modifying this enum.
 */
public enum IndexType {
    /**
     * @see org.neo4j.graphdb.schema.IndexType#FULLTEXT
     */
    FULLTEXT(1),
    /**
     * @see org.neo4j.graphdb.schema.IndexType#LOOKUP
     */
    LOOKUP(2),
    /**
     * @see org.neo4j.graphdb.schema.IndexType#TEXT
     */
    TEXT(3),
    /**
     * @see org.neo4j.graphdb.schema.IndexType#RANGE
     */
    RANGE(4),
    /**
     * @see org.neo4j.graphdb.schema.IndexType#POINT
     */
    POINT(5),
    /**
     * @see org.neo4j.graphdb.schema.IndexType#VECTOR
     */
    VECTOR(6);

    private final int typeNumber;

    IndexType(int typeNumber) {
        this.typeNumber = typeNumber;
    }

    public static IndexType fromPublicApi(org.neo4j.graphdb.schema.IndexType type) {
        if (type == null) {
            return null;
        }

        return switch (type) {
            case FULLTEXT -> FULLTEXT;
            case LOOKUP -> LOOKUP;
            case TEXT -> TEXT;
            case RANGE -> RANGE;
            case POINT -> POINT;
            case VECTOR -> VECTOR;
        };
    }

    public org.neo4j.graphdb.schema.IndexType toPublicApi() {
        return switch (this) {
            case FULLTEXT -> org.neo4j.graphdb.schema.IndexType.FULLTEXT;
            case LOOKUP -> org.neo4j.graphdb.schema.IndexType.LOOKUP;
            case TEXT -> org.neo4j.graphdb.schema.IndexType.TEXT;
            case RANGE -> org.neo4j.graphdb.schema.IndexType.RANGE;
            case POINT -> org.neo4j.graphdb.schema.IndexType.POINT;
            case VECTOR -> org.neo4j.graphdb.schema.IndexType.VECTOR;
        };
    }

    public boolean isLookup() {
        return this == LOOKUP;
    }

    /**
     * To be used in schema name generation to make sure that doesn't change when types are added/removed from the enum.
     */
    public int getTypeNumber() {
        return typeNumber;
    }
}
