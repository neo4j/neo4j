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

import org.neo4j.internal.kernel.api.exceptions.DeletedNodeStillHasRelationshipsException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;

/**
 * Validates data integrity during the prepare phase of {@link TransactionRecordState}.
 */
class IntegrityValidator {
    private final NeoStores neoStores;

    IntegrityValidator(NeoStores neoStores) {
        this.neoStores = neoStores;
    }

    static void validateNodeRecord(NodeRecord record) throws TransactionFailureException {
        if (!record.inUse() && record.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue()) {
            throw new DeletedNodeStillHasRelationshipsException(record.getId());
        }
    }

    void validateTransactionStartKnowledge(long lastCommittedTxWhenTransactionStarted)
            throws TransactionFailureException {
        long latestConstraintIntroducingTx = neoStores.getMetaDataStore().getLatestConstraintIntroducingTx();
        if (lastCommittedTxWhenTransactionStarted < latestConstraintIntroducingTx) {
            // Constraints have changed since the transaction begun

            // This should be a relatively uncommon case, window for this happening is a few milliseconds when an admin
            // explicitly creates a constraint, after the index has been populated. We can improve this later on by
            // replicating the constraint validation logic down here, or rethinking where we validate constraints.
            // For now, we just kill these transactions.
            throw new TransactionFailureException(
                    Status.Transaction.ConstraintsChanged,
                    "Database constraints have changed (txId=%d) after this transaction (txId=%d) started, "
                            + "which is not yet supported. Please retry your transaction to ensure all "
                            + "constraints are executed.",
                    latestConstraintIntroducingTx,
                    lastCommittedTxWhenTransactionStarted);
        }
    }
}
