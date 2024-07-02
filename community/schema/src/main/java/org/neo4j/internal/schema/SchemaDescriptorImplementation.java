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

import static java.util.Objects.requireNonNull;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.internal.schema.PropertySchemaType.COMPLETE_ALL_TOKENS;
import static org.neo4j.internal.schema.PropertySchemaType.ENTITY_TOKENS;
import static org.neo4j.internal.schema.PropertySchemaType.PARTIAL_ANY_TOKEN;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_ID_NAME_LOOKUP;

import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.lock.ResourceType;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.util.Preconditions;

public final class SchemaDescriptorImplementation
        implements SchemaDescriptor,
                LabelSchemaDescriptor,
                RelationTypeSchemaDescriptor,
                FulltextSchemaDescriptor,
                AnyTokenSchemaDescriptor {
    public static final int TOKEN_INDEX_LOCKING_ID = Integer.MAX_VALUE;
    public static final long[] TOKEN_INDEX_LOCKING_IDS = {TOKEN_INDEX_LOCKING_ID};

    private final EntityType entityType;
    private final PropertySchemaType propertySchemaType;
    private final int[] entityTokens;
    private final int[] propertyKeyIds;

    private final SchemaArchetype schemaArchetype;

    /**
     * This constructor is only public so that it can be called directly from the SchemaStore.
     * Use the static methods on {@link SchemaDescriptors} to create the usual kinds of schemas.
     */
    public SchemaDescriptorImplementation(
            EntityType entityType, PropertySchemaType propertySchemaType, int[] entityTokens, int[] propertyKeyIds) {
        this.entityType = requireNonNull(entityType, "EntityType cannot be null.");
        this.propertySchemaType = requireNonNull(propertySchemaType, "PropertySchemaType cannot be null.");
        this.entityTokens = requireNonNull(entityTokens, "Entity tokens array cannot be null.");
        this.propertyKeyIds = requireNonNull(propertyKeyIds, "Property key ids array cannot be null.");

        if (propertySchemaType != ENTITY_TOKENS) {
            validatePropertySchema(entityType, entityTokens, propertyKeyIds);
        } else {
            validateEntityTokenSchema(entityType, entityTokens, propertyKeyIds);
        }

        schemaArchetype = detectArchetype(entityType, propertySchemaType, entityTokens);
    }

    private SchemaArchetype detectArchetype(
            EntityType entityType, PropertySchemaType propertySchemaType, int[] entityTokens) {
        if (entityType == NODE && entityTokens.length == 1 && propertySchemaType == COMPLETE_ALL_TOKENS) {
            return SchemaArchetype.LABEL_PROPERTY;
        }
        if (entityType == RELATIONSHIP && entityTokens.length == 1 && propertySchemaType == COMPLETE_ALL_TOKENS) {
            return SchemaArchetype.RELATIONSHIP_PROPERTY;
        }
        if (propertySchemaType == PARTIAL_ANY_TOKEN) {
            return SchemaArchetype.MULTI_TOKEN;
        }
        if (propertySchemaType == ENTITY_TOKENS) {
            return SchemaArchetype.ANY_TOKEN;
        }
        throw new IllegalArgumentException("Can't detect schema archetype for arguments: " + entityType + " "
                + propertySchemaType + " " + Arrays.toString(entityTokens));
    }

    private static void validatePropertySchema(EntityType entityType, int[] entityTokens, int[] propertyKeyIds) {
        if (entityTokens.length == 0) {
            throw new IllegalArgumentException("Schema descriptor must have at least one "
                    + (entityType == NODE ? "label." : "relationship type."));
        }
        if (propertyKeyIds.length == 0) {
            throw new IllegalArgumentException("Schema descriptor must have at least one property key id.");
        }

        switch (entityType) {
            case NODE -> validateLabelIds(entityTokens);
            case RELATIONSHIP -> validateRelationshipTypeIds(entityTokens);
            default -> throw new IllegalArgumentException("Unknown entity type: " + entityType + ".");
        }
        validatePropertyIds(propertyKeyIds);
    }

    private static void validateEntityTokenSchema(EntityType entityType, int[] entityTokens, int[] propertyKeyIds) {
        if (entityTokens.length != 0) {
            throw new IllegalArgumentException("Schema descriptor with propertySchemaType " + ENTITY_TOKENS
                    + " should not have any specified " + (entityType == NODE ? "labels." : "relationship types."));
        }
        if (propertyKeyIds.length != 0) {
            throw new IllegalArgumentException("Schema descriptor with propertySchemaType " + ENTITY_TOKENS
                    + " should not have any specified property key ids.");
        }
    }

    private static void validatePropertyIds(int... propertyIds) {
        for (int propertyId : propertyIds) {
            if (TokenConstants.ANY_PROPERTY_KEY == propertyId) {
                throw new IllegalArgumentException(
                        "Index schema descriptor can't be created for non existent property.");
            }
        }
    }

    private static void validateRelationshipTypeIds(int... relTypes) {
        for (int relType : relTypes) {
            if (TokenConstants.ANY_RELATIONSHIP_TYPE == relType) {
                throw new IllegalArgumentException(
                        "Index schema descriptor can't be created for non existent relationship type.");
            }
        }
    }

    private static void validateLabelIds(int... labelIds) {
        for (int labelId : labelIds) {
            if (TokenConstants.ANY_LABEL == labelId) {
                throw new IllegalArgumentException("Index schema descriptor can't be created for non existent label.");
            }
        }
    }

    @Override
    public <T extends SchemaDescriptor> boolean isSchemaDescriptorType(Class<T> type) {
        Preconditions.requireNonNull(type, "type argument cannot be null.");
        // TODO: make use of switch with pattern matching of JDK 21 when we do
        //       not support 17 anymore.
        return switch (type.getSimpleName()) {
            case "SchemaDescriptor" -> true;
            case "LabelSchemaDescriptor" -> schemaArchetype == SchemaArchetype.LABEL_PROPERTY;
            case "RelationTypeSchemaDescriptor" -> schemaArchetype == SchemaArchetype.RELATIONSHIP_PROPERTY;
            case "FulltextSchemaDescriptor" -> schemaArchetype == SchemaArchetype.MULTI_TOKEN;
            case "AnyTokenSchemaDescriptor" -> schemaArchetype == SchemaArchetype.ANY_TOKEN;
            default -> false;
        };
    }

    @Override
    public <T extends SchemaDescriptor> T asSchemaDescriptorType(Class<T> type) {
        if (isSchemaDescriptorType(type)) {
            return (T) this;
        }
        throw cannotCastException(type.getSimpleName());
    }

    @Override
    public boolean isLabelSchemaDescriptor() {
        return schemaArchetype == SchemaArchetype.LABEL_PROPERTY;
    }

    @Override
    public LabelSchemaDescriptor asLabelSchemaDescriptor() {
        if (schemaArchetype != SchemaArchetype.LABEL_PROPERTY) {
            throw cannotCastException("LabelSchemaDescriptor");
        }
        return this;
    }

    @Override
    public boolean isRelationshipTypeSchemaDescriptor() {
        return schemaArchetype == SchemaArchetype.RELATIONSHIP_PROPERTY;
    }

    @Override
    public RelationTypeSchemaDescriptor asRelationshipTypeSchemaDescriptor() {
        if (schemaArchetype != SchemaArchetype.RELATIONSHIP_PROPERTY) {
            throw cannotCastException("RelationTypeSchemaDescriptor");
        }
        return this;
    }

    @Override
    public boolean isFulltextSchemaDescriptor() {
        return schemaArchetype == SchemaArchetype.MULTI_TOKEN;
    }

    @Override
    public FulltextSchemaDescriptor asFulltextSchemaDescriptor() {
        if (schemaArchetype != SchemaArchetype.MULTI_TOKEN) {
            throw cannotCastException("FulltextSchemaDescriptor");
        }
        return this;
    }

    @Override
    public boolean isAnyTokenSchemaDescriptor() {
        return schemaArchetype == SchemaArchetype.ANY_TOKEN;
    }

    @Override
    public AnyTokenSchemaDescriptor asAnyTokenSchemaDescriptor() {
        if (schemaArchetype != SchemaArchetype.ANY_TOKEN) {
            throw cannotCastException("AnyTokenSchemaDescriptor");
        }
        return this;
    }

    private IllegalStateException cannotCastException(String descriptorType) {
        return new IllegalStateException("Cannot cast this schema to a " + descriptorType
                + " because it does not match that structure: " + this + ".");
    }

    @Override
    public boolean isAffected(int[] entityTokenIds) {
        for (int id : entityTokens) {
            if (ArrayUtils.contains(entityTokenIds, id)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String userDescription(TokenNameLookup tokenNameLookup) {
        return SchemaUserDescription.forSchema(tokenNameLookup, entityType, entityTokens, propertyKeyIds);
    }

    @Override
    public int[] getPropertyIds() {
        return propertyKeyIds;
    }

    @Override
    public int[] getEntityTokenIds() {
        return entityTokens;
    }

    @Override
    public ResourceType keyType() {
        return entityType == EntityType.NODE ? ResourceType.LABEL : ResourceType.RELATIONSHIP_TYPE;
    }

    @Override
    public EntityType entityType() {
        return entityType;
    }

    @Override
    public PropertySchemaType propertySchemaType() {
        return propertySchemaType;
    }

    @Override
    public long[] lockingKeys() {
        // for AnyToken schema which doesn't have specific token ids lock on max long
        if (isAnyTokenSchemaDescriptor()) {
            return TOKEN_INDEX_LOCKING_IDS;
        }

        int[] tokenIds = getEntityTokenIds();
        int tokenCount = tokenIds.length;
        long[] lockingIds = new long[tokenCount];
        for (int i = 0; i < tokenCount; i++) {
            lockingIds[i] = tokenIds[i];
        }
        Arrays.sort(lockingIds); // Sort to ensure labels are locked and assigned in order.
        return lockingIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaDescriptor that)) {
            return false;
        }
        return entityType == that.entityType()
                && propertySchemaType == that.propertySchemaType()
                && Arrays.equals(entityTokens, that.getEntityTokenIds())
                && Arrays.equals(propertyKeyIds, that.getPropertyIds());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(entityType, propertySchemaType);
        result = 31 * result + Arrays.hashCode(entityTokens);
        result = 31 * result + Arrays.hashCode(propertyKeyIds);
        return result;
    }

    @Override
    public String toString() {
        return userDescription(TOKEN_ID_NAME_LOOKUP);
    }
}
