/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.io.IOException;

import org.neo4j.function.ThrowingLongUnaryOperator;
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
public class TransactionChecksumLookup implements ThrowingLongUnaryOperator<IOException>
{
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;

    public TransactionChecksumLookup( TransactionIdStore transactionIdStore,
            LogicalTransactionStore logicalTransactionStore )
    {
        this.transactionIdStore = transactionIdStore;
        this.logicalTransactionStore = logicalTransactionStore;
    }

    @Override
    public long applyAsLong( long txId ) throws IOException
    {
        // First off see if the requested txId is in fact the last committed transaction.
        // If so then we can extract the checksum directly from the transaction id store.
        TransactionId lastCommittedTransaction = transactionIdStore.getLastCommittedTransaction();
        if ( lastCommittedTransaction.transactionId() == txId )
        {
            return lastCommittedTransaction.checksum();
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
            // OK we couldn't find it there either. So final attempt will be to check for the rare occasion
            // where the txId is the transaction where the last upgrade was performed at. If it is,
            // then we can extract the checksum from transaction id store as well.
            TransactionId lastUpgradeTransaction = transactionIdStore.getUpgradeTransaction();
            if ( lastUpgradeTransaction.transactionId() == txId )
            {
                return lastUpgradeTransaction.checksum();
            }

            // So we truly couldn't find the checksum for this txId, go ahead and throw
            throw withMessage( e, e.getMessage() + " | transaction id store says last transaction is " +
                    lastCommittedTransaction + " and last upgrade transaction is " +
                    lastUpgradeTransaction );
        }
    }
}
