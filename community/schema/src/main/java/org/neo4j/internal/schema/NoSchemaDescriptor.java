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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.lock.ResourceType;

/**
 * A schema descriptor sentinel used for signalling the absence of a real schema descriptor.
 * <p>
 * The instance is acquired via the {@link SchemaDescriptors#noSchema()} method.
 */
final class NoSchemaDescriptor implements SchemaDescriptor {
    private static final String CAST_ERROR_FMT = "NO_SCHEMA cannot be cast to a %s.";
    static final SchemaDescriptor NO_SCHEMA = new NoSchemaDescriptor();

    private NoSchemaDescriptor() {}

    @Override
    public <T extends SchemaDescriptor> boolean isSchemaDescriptorType(Class<T> type) {
        return false;
    }

    @Override
    public <T extends SchemaDescriptor> T asSchemaDescriptorType(Class<T> type) {
        throw new IllegalStateException(CAST_ERROR_FMT.formatted(type.getSimpleName()));
    }

    @Override
    public boolean isFulltextSchemaDescriptor() {
        return false;
    }

    @Override
    public FulltextSchemaDescriptor asFulltextSchemaDescriptor() {
        throw new IllegalStateException(CAST_ERROR_FMT.formatted("FulltextSchemaDescriptor"));
    }

    @Override
    public boolean isAnyTokenSchemaDescriptor() {
        return false;
    }

    @Override
    public AnyTokenSchemaDescriptor asAnyTokenSchemaDescriptor() {
        throw new IllegalStateException(CAST_ERROR_FMT.formatted("AnyTokenSchemaDescriptor"));
    }

    @Override
    public boolean isAffected(int[] entityIds) {
        return false;
    }

    @Override
    public String userDescription(TokenNameLookup tokenNameLookup) {
        return "NO_SCHEMA";
    }

    @Override
    public int[] getPropertyIds() {
        return EMPTY_INT_ARRAY;
    }

    @Override
    public int[] getEntityTokenIds() {
        return EMPTY_INT_ARRAY;
    }

    @Override
    public ResourceType keyType() {
        return null;
    }

    @Override
    public EntityType entityType() {
        return null;
    }

    @Override
    public PropertySchemaType propertySchemaType() {
        return null;
    }

    @Override
    public long[] lockingKeys() {
        return EMPTY_LONG_ARRAY;
    }
}
