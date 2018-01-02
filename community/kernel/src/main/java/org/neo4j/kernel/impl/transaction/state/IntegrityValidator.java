/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.SchemaRule;

/**
 * Validates data integrity during the prepare phase of {@link TransactionRecordState}.
 */
public class IntegrityValidator
{
    private final NeoStores neoStores;
    private final IndexingService indexes;

    public IntegrityValidator( NeoStores neoStores, IndexingService indexes )
    {
        this.neoStores = neoStores;
        this.indexes = indexes;
    }

    public void validateNodeRecord( NodeRecord record ) throws TransactionFailureException
    {
        if ( !record.inUse() && record.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            throw new TransactionFailureException( Status.Transaction.ValidationFailed,
                    "Node record " + record + " still has relationships" );
        }
    }

    public void validateTransactionStartKnowledge( long lastCommittedTxWhenTransactionStarted )
            throws TransactionFailureException
    {
        long latestConstraintIntroducingTx = neoStores.getMetaDataStore().getLatestConstraintIntroducingTx();
        if ( lastCommittedTxWhenTransactionStarted < latestConstraintIntroducingTx )
        {
            // Constraints have changed since the transaction begun

            // This should be a relatively uncommon case, window for this happening is a few milliseconds when an admin
            // explicitly creates a constraint, after the index has been populated. We can improve this later on by
            // replicating the constraint validation logic down here, or rethinking where we validate constraints.
            // For now, we just kill these transactions.
            throw new TransactionFailureException( Status.Transaction.ConstraintsChanged,
                            "Database constraints have changed (txId=%d) after this transaction (txId=%d) started, " +
                            "which is not yet supported. Please retry your transaction to ensure all " +
                            "constraints are executed.", latestConstraintIntroducingTx,
                            lastCommittedTxWhenTransactionStarted );
        }
    }

    public void validateSchemaRule( SchemaRule schemaRule ) throws TransactionFailureException
    {
        if ( schemaRule instanceof UniquePropertyConstraintRule )
        {
            try
            {
                indexes.validateIndex( ((UniquePropertyConstraintRule)schemaRule).getOwnedIndex() );
            }
            catch ( ConstraintVerificationFailedKernelException e )
            {
                throw new TransactionFailureException( Status.Transaction.ValidationFailed, e, "Index validation failed" );
            }
            catch ( IndexNotFoundKernelException | IndexPopulationFailedKernelException e )
            {
                // We don't expect this to occur, and if they do, it is because we are in a very bad state - out of
                // disk or index corruption, or similar. This will kill the database such that it can be shut down
                // and have recovery performed. It's the safest bet to avoid loosing data.
                throw new TransactionFailureException( Status.Transaction.ValidationFailed, e, "Index population failure" );
            }
        }
    }
}
