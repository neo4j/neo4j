/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.catchup.tx.edge;

import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ApplyPulledTransactions implements TxPullResponseListener
{
    private final Supplier<TransactionApplier> transactionApplierSupplier;
    private final Log log;
    private final Supplier<TransactionIdStore> transactionIdStoreSupplier;

    public ApplyPulledTransactions( LogProvider logProvider,
                                    Supplier<TransactionApplier> transactionApplierSupplier,
                                    Supplier<TransactionIdStore> transactionIdStoreSupplier )
    {
        this.transactionApplierSupplier = transactionApplierSupplier;
        this.transactionIdStoreSupplier = transactionIdStoreSupplier;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void onTxReceived( TxPullResponse tx )
    {
        if ( tx.tx().getCommitEntry().getTxId() <= transactionIdStoreSupplier.get().getLastCommittedTransactionId() )
        {
            return;
        }

        try
        {
            transactionApplierSupplier.get().appendToLogAndApplyToStore( tx.tx() );
        }
        catch ( TransactionFailureException e )
        {
            log.error( "Failed to apply transaction.", e );
        }
    }
}
