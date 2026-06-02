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
package org.neo4j.internal.recordstorage;

import static org.neo4j.kernel.KernelVersion.LATEST_SCHEMA_CHANGE;
import static org.neo4j.kernel.KernelVersion.VERSION_NODE_VECTOR_INDEX_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_RELATIONSHIP_ENDPOINT_LABEL_AND_LABEL_EXISTENCE_CONSTRAINTS_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_TYPE_CONSTRAINTS_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_UNIONS_AND_LIST_TYPE_CONSTRAINTS_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_VECTOR_2_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_VECTOR_INDEX_SINGLE_STAGE_FILTERING;
import static org.neo4j.kernel.KernelVersion.VERSION_VECTOR_TYPE_INTRODUCED;

import org.neo4j.internal.kernel.api.exceptions.DeletedNodeStillHasRelationshipsException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.TypeRepresentation;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;

/**
 * Validates data integrity during the prepare phase of {@link TransactionRecordState}.
 */
class IntegrityValidator {
    static void validateNodeRecord(NodeRecord record) throws TransactionFailureException {
        if (!record.inUse() && record.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue()) {
            throw DeletedNodeStillHasRelationshipsException.nodeStillHasRelationships(record.getId());
        }
    }

    /**
     * Validates that the kernelVersion is now up to what it needs to be to allow new types.
     * Things should only have been let through this far if the runtime version was high enough
     * and there was hope that the upgrade transaction would run before this. If for some reason
     * the upgrade transaction has not succeeded we need to abort this commit with unsupported
     * features now.
     */
    static void validateSchemaRule(SchemaRule schemaRule, KernelVersion kernelVersion)
            throws TransactionFailureException {
        if (kernelVersion.isAtLeast(LATEST_SCHEMA_CHANGE)) {
            return;
        }

        if (schemaRule instanceof IndexDescriptor index) {
            final String schemaType = "index";

            SchemaDescriptor schema = index.schema();
            if (index.getIndexType() == IndexType.VECTOR) {
                switch (schema.entityType()) {
                    case NODE -> {
                        if (kernelVersion.isLessThan(VERSION_NODE_VECTOR_INDEX_INTRODUCED)) {
                            throw upgradeNeededForSchemaRule(
                                    schemaType, index, kernelVersion, VERSION_NODE_VECTOR_INDEX_INTRODUCED);
                        }
                    }
                    case RELATIONSHIP -> {
                        if (kernelVersion.isLessThan(VERSION_VECTOR_2_INTRODUCED)) {
                            throw upgradeNeededForSchemaRule(
                                    schemaType, index, kernelVersion, VERSION_VECTOR_2_INTRODUCED);
                        }
                    }
                }

                if ((schema.getEntityTokenIds().length > 1 || schema.getPropertyIds().length > 1)
                        && kernelVersion.isLessThan(VERSION_VECTOR_INDEX_SINGLE_STAGE_FILTERING)) {
                    throw upgradeNeededForSchemaRule(
                            schemaType, index, kernelVersion, VERSION_VECTOR_INDEX_SINGLE_STAGE_FILTERING);
                }
            }

        } else if (schemaRule instanceof ConstraintDescriptor constraint) {
            final String schemaType = "constraint";

            if ((constraint.isRelationshipUniquenessConstraint() || constraint.isRelationshipKeyConstraint())
                    && kernelVersion.isLessThan(VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED)) {
                throw upgradeNeededForSchemaRule(
                        schemaType, constraint, kernelVersion, VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED);
            }

            if (constraint.isPropertyTypeConstraint()) {
                if (kernelVersion.isLessThan(VERSION_TYPE_CONSTRAINTS_INTRODUCED)) {
                    throw upgradeNeededForSchemaRule(
                            schemaType, constraint, kernelVersion, VERSION_TYPE_CONSTRAINTS_INTRODUCED);
                }

                PropertyTypeSet propertyType =
                        constraint.asPropertyTypeConstraint().propertyType();
                if ((TypeRepresentation.isUnion(propertyType) || TypeRepresentation.hasListTypes(propertyType))
                        && kernelVersion.isLessThan(VERSION_UNIONS_AND_LIST_TYPE_CONSTRAINTS_INTRODUCED)) {
                    throw upgradeNeededForSchemaRule(
                            schemaType, constraint, kernelVersion, VERSION_UNIONS_AND_LIST_TYPE_CONSTRAINTS_INTRODUCED);
                }
                if ((TypeRepresentation.hasVectorTypes(propertyType))
                        && kernelVersion.isLessThan(VERSION_VECTOR_TYPE_INTRODUCED)) {
                    throw upgradeNeededForSchemaRule(
                            schemaType, constraint, kernelVersion, VERSION_VECTOR_TYPE_INTRODUCED);
                }
            }

            if (constraint.isRelationshipEndpointLabelConstraint() || constraint.isNodeLabelExistenceConstraint()) {
                if (kernelVersion.isLessThan(
                        VERSION_RELATIONSHIP_ENDPOINT_LABEL_AND_LABEL_EXISTENCE_CONSTRAINTS_INTRODUCED)) {
                    throw upgradeNeededForSchemaRule(
                            schemaType,
                            constraint,
                            kernelVersion,
                            VERSION_RELATIONSHIP_ENDPOINT_LABEL_AND_LABEL_EXISTENCE_CONSTRAINTS_INTRODUCED);
                }
            }

        } else {
            throw new IllegalArgumentException(
                    "Unknown %s. Provided: %s".formatted(SchemaRule.class.getSimpleName(), schemaRule));
        }
    }

    private static TransactionFailureException upgradeNeededForSchemaRule(
            String schemaType, SchemaRule schemaRule, KernelVersion actualVersion, KernelVersion requiredVersion) {
        return TransactionFailureException.internalError(
                Status.General.UpgradeRequired,
                IntegrityValidator.class.getSimpleName(),
                "Operations on %s '%s' not allowed. "
                        + "Required kernel version for this transaction is %s, but actual version was %s.",
                schemaType,
                schemaRule,
                requiredVersion.name(),
                actualVersion.name());
    }
}
