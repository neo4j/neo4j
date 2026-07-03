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

import static org.neo4j.util.Preconditions.requireNonNull;

import java.util.Optional;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.hashing.HashFunction;
import org.neo4j.internal.schema.constraints.RelationshipEndpointLabelConstraintDescriptor;
import org.neo4j.util.Preconditions;

public class SchemaNameUtil {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static String sanitiseName(Optional<String> name) {
        if (name.isPresent()) {
            return sanitiseName(name.get());
        }
        throw new IllegalArgumentException("Schema rules must have names.");
    }

    public static String sanitiseName(String name) {
        requireNonNull(name, "Schema rule name cannot be null.");
        name = name.trim();
        if (name.isBlank()) {
            throw new IllegalArgumentException(
                    "Schema rule name cannot be the empty string or only contain whitespace.");
        }
        if (name.indexOf('\0') != -1) {
            throw new IllegalArgumentException(
                    "Schema rule names are not allowed to contain null-bytes: '" + name + "'.");
        }
        if (ReservedSchemaRuleNames.contains(name)) {
            throw new IllegalArgumentException("The index name '" + name + "' is reserved, and cannot be used. "
                    + "The reserved names are " + ReservedSchemaRuleNames.getReservedNames() + ".");
        }
        return name;
    }

    /**
     * Generate a <em>deterministic</em> name for the given {@link SchemaDescriptorSupplier}.
     * Only {@link SchemaRule} implementations, and {@link IndexPrototype}, are supported arguments for the schema descriptor supplier.
     * This only accepts schemas that does not target any entity tokens or properties. Otherwise, use
     * {@link #generateName(SchemaDescriptorSupplier, TokenNameLookup)}.
     *
     * @param rule The {@link SchemaDescriptorSupplier} to generate a name for.
     * @return A name.
     */
    public static String generateName(SchemaDescriptorSupplier rule) {
        Preconditions.checkArgument(
                rule.schema().getEntityTokenIds().length == 0,
                "Schema should target no entity tokens (labels, relationship types).");
        Preconditions.checkArgument(
                rule.schema().getPropertyIds().length == 0, "Schema should target no property keys.");
        return generateName(rule, null);
    }

    /**
     * Generate a <em>deterministic</em> name for the given {@link SchemaDescriptorSupplier}.
     * Only {@link SchemaRule} implementations, and {@link IndexPrototype}, are supported arguments for the schema descriptor supplier.
     *
     * @param rule The {@link SchemaDescriptorSupplier} to generate a name for.
     * @param tokenNameLookup The {@link TokenNameLookup} to fetch the names for tokens.
     * @return A name.
     */
    public static String generateName(SchemaDescriptorSupplier rule, TokenNameLookup tokenNameLookup) {
        // NOTE to future maintainers: You probably want to avoid touching this function.
        // Last time this was changed, we had some 400+ tests to update.
        HashFunction hf = HashFunction.incrementalXXH64();

        // common strong seed for distributing values across 64-bit space
        // hex(floor(pow(2, 64) / goldenRatio))
        long key = hf.initialise(0x9E3779B97F4A7C15L);

        SchemaDescriptor schema = rule.schema();
        int[] entityTokenIds = schema.getEntityTokenIds();
        int[] propertyKeyIds = schema.getPropertyIds();
        key = hf.update(key, schema.entityType().ordinal());
        key = hf.update(
                key,
                SchemaDescriptorImplementation.effectiveSchemaPatternMatchingTypeFeature(
                        schema.schemaPatternMatchingType(), propertyKeyIds));
        key = switch (schema.entityType()) {
            case NODE ->
                hf.updateWithArray(
                        key,
                        entityTokenIds,
                        id -> tokenNameLookup.labelGetName(id).hashCode());
            case RELATIONSHIP ->
                hf.updateWithArray(
                        key,
                        entityTokenIds,
                        id -> tokenNameLookup.relationshipTypeGetName(id).hashCode());
        };
        key = hf.updateWithArray(
                key,
                propertyKeyIds,
                id -> tokenNameLookup.propertyKeyGetName(id).hashCode());

        key = hf.update(key, Boolean.hashCode(rule instanceof ConstraintDescriptor));
        return switch (rule) {
            case IndexRef<?> index -> {
                key = hf.update(key, Boolean.hashCode(index.isUnique()));
                key = hf.update(key, index.getIndexType().getTypeNumber());
                yield "index_%x".formatted(hf.toInt(hf.finalise(key)));
            }

            case ConstraintDescriptor constraint -> {
                key = hf.update(key, constraint.type().ordinal());
                if (constraint.isIndexBackedConstraint()) {
                    key = hf.update(
                            key,
                            constraint.asIndexBackedConstraint().indexType().getTypeNumber());
                } else if (constraint.enforcesPropertyType()) {
                    key = hf.update(
                            key,
                            constraint.asPropertyTypeConstraint().propertyType().hashCode());
                } else if (constraint.isRelationshipEndpointLabelConstraint()) {
                    RelationshipEndpointLabelConstraintDescriptor relEndpointLabelConstraint =
                            constraint.asRelationshipEndpointLabelConstraint();
                    key = hf.update(
                            key,
                            tokenNameLookup
                                    .labelGetName(relEndpointLabelConstraint.endpointLabelId())
                                    .hashCode());
                    key = hf.update(
                            key, relEndpointLabelConstraint.endpointType().ordinal());
                } else if (constraint.isNodeLabelExistenceConstraint()) {
                    key = hf.update(
                            key,
                            tokenNameLookup
                                    .labelGetName(constraint
                                            .asNodeLabelExistenceConstraint()
                                            .requiredLabelId())
                                    .hashCode());
                }

                yield "constraint_%x".formatted(hf.toInt(hf.finalise(key)));
            }

            default ->
                throw new IllegalArgumentException("Don't know how to generate a name for this %s implementation: %s."
                        .formatted(SchemaDescriptorSupplier.class.getSimpleName(), rule));
        };
    }
}
