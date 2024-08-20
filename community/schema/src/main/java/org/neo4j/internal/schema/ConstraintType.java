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

import static org.neo4j.common.EntityType.NODE;

import org.neo4j.common.EntityType;

/**
 * The enum of all the types of constraints that we support.
 * This is the internal version of {@link org.neo4j.graphdb.schema.ConstraintType}.
 * <p>
 * NOTE: The ordinal is used in the hash function for the auto-generated SchemaRule names, so avoid changing the ordinals when modifying this enum.
 */
public enum ConstraintType {
    UNIQUE(true, false, false),
    EXISTS(false, true, false),
    UNIQUE_EXISTS(true, true, false),
    PROPERTY_TYPE(false, false, true),
    ENDPOINT(false, false, false),
    LABEL_COEXISTENCE(false, false, false);

    private final boolean isUnique;
    private final boolean mustExist;
    private final boolean isType;

    ConstraintType(boolean isUnique, boolean mustExist, boolean isType) {
        this.isUnique = isUnique;
        this.mustExist = mustExist;
        this.isType = isType;
    }

    public boolean enforcesUniqueness() {
        return isUnique;
    }

    public boolean enforcesPropertyExistence() {
        return mustExist;
    }

    public boolean enforcesPropertyType() {
        return isType;
    }

    public String userDescription(EntityType entityType) {
        String name = entityType.name();
        return switch (this) {
            case EXISTS -> name + " PROPERTY EXISTENCE";
            case UNIQUE -> entityType == NODE ? "UNIQUENESS" : name + " UNIQUENESS";
            case UNIQUE_EXISTS -> name + " KEY";
            case PROPERTY_TYPE -> name + " PROPERTY TYPE";
            case ENDPOINT -> name + " ENDPOINT";
            case LABEL_COEXISTENCE -> name + " LABEL COEXISTENCE";
        };
    }
}
