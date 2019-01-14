/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha;

import java.io.IOException;

import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.neo4j.helpers.Exceptions.withMessage;

/**
 * Transaction meta data can normally be looked up using {@link LogicalTransactionStore#getMetadataFor(long)}.
 * The exception to that is when there are no transaction logs for the database, for example after a migration
 * and we're looking up the checksum for transaction the migration was performed at. In that case we have to
 * extract that checksum directly from {@link TransactionIdStore}, since it's not in any transaction log,
 * at least not at the time of writing this class.
 */
public class TransactionChecksumLookup
{
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;
    private TransactionId upgradeTransaction;

    public TransactionChecksumLookup( TransactionIdStore transactionIdStore,
            LogicalTransactionStore logicalTransactionStore )
    {
        this.transactionIdStore = transactionIdStore;
        this.logicalTransactionStore = logicalTransactionStore;
        this.upgradeTransaction = transactionIdStore.getUpgradeTransaction();
    }

    public long lookup( long txId ) throws IOException
    {
        // First off see if the requested txId is in fact the last committed transaction.
        // If so then we can extract the checksum directly from the transaction id store.
        TransactionId lastCommittedTransaction = transactionIdStore.getLastCommittedTransaction();
        if ( lastCommittedTransaction.transactionId() == txId )
        {
            return lastCommittedTransaction.checksum();
        }

        // Check if the requested txId is upgrade transaction
        // if so then use checksum form transaction id store.
        // That checksum can take specific values that should not be re-evaluated.
        if ( upgradeTransaction.transactionId() == txId )
        {
            return upgradeTransaction.checksum();
        }

        // It wasn't, so go look for it in the transaction store.
        // Intentionally let potentially thrown IOException (and NoSuchTransactionException) be thrown
        // from this call below, it's part of the contract of this method.
        try
        {
            return logicalTransactionStore.getMetadataFor( txId ).getChecksum();
        }
        catch ( NoSuchTransactionException e )
        {
            // So we truly couldn't find the checksum for this txId, go ahead and throw
            throw withMessage( e, e.getMessage() + " | transaction id store says last transaction is " +
                    lastCommittedTransaction + " and last upgrade transaction is " +
                    upgradeTransaction );
        }
    }
}
