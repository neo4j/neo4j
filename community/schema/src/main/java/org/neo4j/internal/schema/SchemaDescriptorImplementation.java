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
import static org.neo4j.internal.schema.SchemaPatternMatchingType.COMPLETE_ALL_TOKENS;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.ENTITY_TOKENS;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.PARTIAL_ANY_TOKEN;
import static org.neo4j.internal.schema.SchemaPatternMatchingType.SINGLE_ENTITY_TOKEN;
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
                AnyTokenSchemaDescriptor,
                RelationshipEndpointSchemaDescriptor {
    public static final int TOKEN_INDEX_LOCKING_ID = Integer.MAX_VALUE;
    public static final long[] TOKEN_INDEX_LOCKING_IDS = {TOKEN_INDEX_LOCKING_ID};

    private final EntityType entityType;
    private final SchemaPatternMatchingType schemaPatternMatchingType;
    private final int[] entityTokens;
    private final int[] propertyKeyIds;

    private final SchemaArchetype schemaArchetype;

    /**
     * This constructor is only public so that it can be called directly from the SchemaStore.
     * Use the static methods on {@link SchemaDescriptors} to create the usual kinds of schemas.
     */
    public SchemaDescriptorImplementation(
            EntityType entityType,
            SchemaPatternMatchingType schemaPatternMatchingType,
            int[] entityTokens,
            int[] propertyKeyIds) {
        this.entityType = requireNonNull(entityType, "EntityType cannot be null.");
        this.schemaPatternMatchingType =
                requireNonNull(schemaPatternMatchingType, "Schema pattern matching type cannot be null.");
        this.entityTokens = requireNonNull(entityTokens, "Entity tokens array cannot be null.");
        this.propertyKeyIds = requireNonNull(propertyKeyIds, "Property key ids array cannot be null.");

        switch (schemaPatternMatchingType) {
            case SINGLE_ENTITY_TOKEN -> validateSingleEntityTokenSchema(entityTokens, propertyKeyIds);
            case ENTITY_TOKENS -> validateEntityTokensSchema(entityType, entityTokens, propertyKeyIds);
            default -> validatePropertySchema(entityType, entityTokens, propertyKeyIds);
        }

        schemaArchetype = detectArchetype(entityType, schemaPatternMatchingType, entityTokens);
    }

    private static void validateSingleEntityTokenSchema(int[] entityTokens, int[] propertyKeyIds) {
        if (entityTokens.length != 1) {
            throw new IllegalArgumentException("Schema descriptor of schema pattern matching type "
                    + SINGLE_ENTITY_TOKEN + " must have exactly one entity token.");
        }

        if (entityTokens[0] < 0) {
            throw new IllegalArgumentException("Schema descriptor of schema pattern matching type "
                    + SINGLE_ENTITY_TOKEN + " must not have a negative entity token id");
        }

        if (propertyKeyIds.length != 0) {
            throw new IllegalArgumentException("Schema descriptor of schema pattern matching type "
                    + SINGLE_ENTITY_TOKEN + " must not have any property key ids");
        }
    }

    private SchemaArchetype detectArchetype(
            EntityType entityType, SchemaPatternMatchingType schemaPatternMatchingType, int[] entityTokens) {

        if (entityTokens.length == 1 && schemaPatternMatchingType == COMPLETE_ALL_TOKENS) {
            if (entityType == NODE) {
                return SchemaArchetype.LABEL_PROPERTY;
            } else if (entityType == RELATIONSHIP) {
                return SchemaArchetype.RELATIONSHIP_PROPERTY;
            }
        } else if (schemaPatternMatchingType == PARTIAL_ANY_TOKEN) {
            return SchemaArchetype.MULTI_TOKEN;
        } else if (schemaPatternMatchingType == ENTITY_TOKENS) {
            return SchemaArchetype.ANY_TOKEN;
        } else if (schemaPatternMatchingType == SINGLE_ENTITY_TOKEN) {
            if (entityType == RELATIONSHIP) {
                return SchemaArchetype.SINGLE_RELATIONSHIP;
            }
        }
        throw new IllegalArgumentException("Can't detect schema archetype for arguments: " + entityType + " "
                + schemaPatternMatchingType + " " + Arrays.toString(entityTokens));
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

    private static void validateEntityTokensSchema(EntityType entityType, int[] entityTokens, int[] propertyKeyIds) {
        if (entityTokens.length != 0) {
            throw new IllegalArgumentException("Schema descriptor with schema pattern matching type " + ENTITY_TOKENS
                    + " should not have any specified " + (entityType == NODE ? "labels." : "relationship types."));
        }
        if (propertyKeyIds.length != 0) {
            throw new IllegalArgumentException("Schema descriptor with schema pattern matching type " + ENTITY_TOKENS
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
            case "RelationshipEndpointSchemaDescriptor" -> schemaArchetype == SchemaArchetype.SINGLE_RELATIONSHIP;
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
    public SchemaPatternMatchingType schemaPatternMatchingType() {
        return schemaPatternMatchingType;
    }

    @Override
    public long[] lockingKeys() {
        // for AnyToken schema which doesn't have specific token ids lock on max long
        if (schemaArchetype == SchemaArchetype.ANY_TOKEN) {
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
                && schemaPatternMatchingType == that.schemaPatternMatchingType()
                && Arrays.equals(entityTokens, that.getEntityTokenIds())
                && Arrays.equals(propertyKeyIds, that.getPropertyIds());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(entityType, schemaPatternMatchingType);
        result = 31 * result + Arrays.hashCode(entityTokens);
        result = 31 * result + Arrays.hashCode(propertyKeyIds);
        return result;
    }

    @Override
    public String toString() {
        return userDescription(TOKEN_ID_NAME_LOOKUP);
    }

    /**
     * Currently there are 4 schema archetypes:
     * 1. LABEL_PROPERTY - schema that describes exactly one label, and one or more properties.
     *      This schema matches nodes that are labeled with specified label, and have all specified properties.
     * 2. RELATIONSHIP_PROPERTY - schema that describes exactly one relationship type, and one or more properties.
     *      This schema matches relationship of specified type that have all specified properties.
     * 3. MULTI_TOKEN - schema that describes at least one label or type, and one or more properties.
     *      This schema matches node/relationship if at least one of it's label/type is specified by the schema,
     *      and if node/relationship has at least one property specified by the schema.
     *      I.e. fulltext indexes are described by this kind of schema
     * 4. ANY_TOKEN - schema that describes any labels or any relationship types, not specifying any properties.
     *      Any labeled node or any relationship type is matched by this schema
     * 5. SINGLE_RELATIONSHIP - schema that describes exactly one relationship type and no properties.
     *      This schema matches relationships of specified type
     */
    private enum SchemaArchetype {
        LABEL_PROPERTY,
        RELATIONSHIP_PROPERTY,
        MULTI_TOKEN,
        ANY_TOKEN,
        SINGLE_RELATIONSHIP
    }
}
