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
import static org.neo4j.common.EntityType.RELATIONSHIP;

import java.util.StringJoiner;
import java.util.function.IntFunction;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.string.Mask;
import org.neo4j.token.api.TokenIdPrettyPrinter;

public final class SchemaUserDescription {
    public static final String TOKEN_LABEL = "<any-labels>";
    public static final String TOKEN_REL_TYPE = "<any-types>";

    private SchemaUserDescription() {}

    static String forSchema(
            TokenNameLookup tokenNameLookup, EntityType entityType, int[] entityTokens, int[] propertyKeyIds) {
        String prefix = entityType == RELATIONSHIP ? "()-[" : "(";
        String suffix = entityType == RELATIONSHIP ? "]-()" : ")";

        // Token indexes, that works on all entity tokens, have no specified entityTokens or propertyKeyIds.
        if (entityTokens.length == 0 && propertyKeyIds.length == 0) {
            String entityTokenType = ":" + (entityType == RELATIONSHIP ? TOKEN_REL_TYPE : TOKEN_LABEL);
            return prefix + entityTokenType + suffix;
        }

        IntFunction<String> lookup =
                entityType == NODE ? tokenNameLookup::labelGetName : tokenNameLookup::relationshipTypeGetName;
        return prefix + TokenIdPrettyPrinter.niceEntityLabels(lookup, entityTokens) + " "
                + TokenIdPrettyPrinter.niceProperties(tokenNameLookup, propertyKeyIds, '{', '}') + suffix;
    }

    static String forPrototype(
            TokenNameLookup tokenNameLookup,
            String name,
            String indexType,
            SchemaDescriptor schema,
            IndexProviderDescriptor indexProvider) {
        StringJoiner joiner = new StringJoiner(", ", "Index( ", " )");
        addPrototypeParams(tokenNameLookup, name, indexType, schema, indexProvider, joiner, Mask.NO);
        return joiner.toString();
    }

    public static String forIndex(
            TokenNameLookup tokenNameLookup,
            long id,
            String name,
            String indexType,
            SchemaDescriptor schema,
            IndexProviderDescriptor indexProvider,
            Long owningConstraintId) {
        return forIndex(tokenNameLookup, id, name, indexType, schema, indexProvider, owningConstraintId, Mask.NO);
    }

    public static String forIndex(
            TokenNameLookup tokenNameLookup,
            long id,
            String name,
            String indexType,
            SchemaDescriptor schema,
            IndexProviderDescriptor indexProvider,
            Long owningConstraintId,
            Mask mask) {
        StringJoiner joiner = new StringJoiner(", ", "Index( ", " )");
        joiner.add("id=" + id);
        addPrototypeParams(tokenNameLookup, name, indexType, schema, indexProvider, joiner, mask);
        if (owningConstraintId != null) {
            joiner.add("owningConstraint=" + owningConstraintId);
        }
        return joiner.toString();
    }

    public static String forConstraint(
            TokenNameLookup tokenNameLookup,
            long id,
            String name,
            ConstraintType type,
            SchemaDescriptor schema,
            Long ownedIndex,
            PropertyTypeSet propertyType) {
        return forConstraint(tokenNameLookup, id, name, type, schema, ownedIndex, propertyType, Mask.NO);
    }

    public static String forConstraint(
            TokenNameLookup tokenNameLookup,
            long id,
            String name,
            ConstraintType type,
            SchemaDescriptor schema,
            Long ownedIndex,
            PropertyTypeSet propertyType,
            Mask mask) {
        StringJoiner joiner = new StringJoiner(", ", "Constraint( ", " )");
        maybeAddId(id, joiner);
        maybeAddName(name, joiner, mask);
        addType(constraintType(type, schema.entityType()), joiner);
        addSchema(tokenNameLookup, schema, joiner);
        if (ownedIndex != null) {
            joiner.add("ownedIndex=" + ownedIndex);
        }
        maybeAddAllowedPropertyTypes(propertyType, joiner);
        return joiner.toString();
    }

    private static String constraintType(ConstraintType type, EntityType entityType) {
        return switch (type) {
            case EXISTS -> entityType.name() + " PROPERTY EXISTENCE";
            case UNIQUE -> entityType == NODE ? "UNIQUENESS" : entityType.name() + " UNIQUENESS";
            case UNIQUE_EXISTS -> entityType.name() + " KEY";
            case PROPERTY_TYPE -> entityType.name() + " PROPERTY TYPE";
        };
    }

    private static void maybeAddId(long id, StringJoiner joiner) {
        if (id != ConstraintDescriptor.NO_ID) {
            joiner.add("id=" + id);
        }
    }

    private static void maybeAddName(String name, StringJoiner joiner, Mask mask) {
        if (name != null) {
            joiner.add("name='" + mask.filter(name) + "'");
        }
    }

    private static void maybeAddAllowedPropertyTypes(PropertyTypeSet propertyType, StringJoiner joiner) {
        if (propertyType != null) {
            joiner.add("propertyType=" + propertyType.userDescription());
        }
    }

    private static void addPrototypeParams(
            TokenNameLookup tokenNameLookup,
            String name,
            String indexType,
            SchemaDescriptor schema,
            IndexProviderDescriptor indexProvider,
            StringJoiner joiner,
            Mask mask) {
        maybeAddName(name, joiner, mask);
        addType(indexType, joiner);
        addSchema(tokenNameLookup, schema, joiner);
        joiner.add("indexProvider='" + indexProvider.name() + "'");
    }

    private static void addType(String type, StringJoiner joiner) {
        joiner.add("type='" + type + "'");
    }

    private static void addSchema(TokenNameLookup tokenNameLookup, SchemaDescriptor schema, StringJoiner joiner) {
        joiner.add("schema=" + schema.userDescription(tokenNameLookup));
    }

    public static final TokenNameLookup TOKEN_ID_NAME_LOOKUP = new TokenNameLookup() {
        @Override
        public String labelGetName(int labelId) {
            return "Label[" + labelId + "]";
        }

        @Override
        public String relationshipTypeGetName(int relationshipTypeId) {
            return "RelationshipType[" + relationshipTypeId + "]";
        }

        @Override
        public String propertyKeyGetName(int propertyKeyId) {
            return "PropertyKey[" + propertyKeyId + "]";
        }
    };
}
