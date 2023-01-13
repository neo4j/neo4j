/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.recordstorage;

import static org.neo4j.kernel.KernelVersion.VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED;

import org.neo4j.internal.kernel.api.exceptions.DeletedNodeStillHasRelationshipsException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaRule;
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
            throw new DeletedNodeStillHasRelationshipsException(record.getId());
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
        // Yes!, this never does anything. Until relationship uniqueness constraints are enabled
        // this creating during rolling upgrade block is not needed yet but to be able to test the upgrade
        // easily before the enabling we keep this around
        if (false
                && kernelVersion.isLessThan(VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED)
                && schemaRule instanceof ConstraintDescriptor constraint) {
            if (constraint.isRelationshipKeyConstraint() || constraint.isRelationshipUniquenessConstraint()) {
                throw new TransactionFailureException(
                        Status.General.UpgradeRequired,
                        "Operation on constraint '%s' not allowed. "
                                + "Required kernel version for this transaction is %s, but actual version was %s.",
                        constraint,
                        VERSION_REL_UNIQUE_CONSTRAINTS_INTRODUCED.name(),
                        kernelVersion.name());
            }
        }
    }
}
