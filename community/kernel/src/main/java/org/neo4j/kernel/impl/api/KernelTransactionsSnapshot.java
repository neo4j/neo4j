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
package org.neo4j.kernel.impl.api;

import java.util.Set;

import org.neo4j.internal.id.IdController;

/**
 * An instance of this class can get a snapshot of all currently running transactions and be able to tell
 * later if all transactions which were running when it was constructed have closed.
 * <p>
 * Creating a snapshot creates a list and one additional book keeping object per open transaction.
 * No thread doing normal transaction work should create snapshots, only threads that monitor transactions.
 */
public class KernelTransactionsSnapshot implements IdController.IdFreeCondition
{
    private Tx relevantTransactions;

    KernelTransactionsSnapshot( Set<KernelTransactionStamp> transactionStamps )
    {
        Tx head = null;
        for ( KernelTransactionStamp stamp : transactionStamps )
        {
            if ( stamp.isOpen() )
            {
                Tx current = new Tx( stamp );
                if ( head != null )
                {
                    current.next = head;
                    head = current;
                }
                else
                {
                    head = current;
                }
            }
        }
        relevantTransactions = head;
    }

    @Override
    public boolean eligibleForFreeing()
    {
        while ( relevantTransactions != null )
        {
            if ( !relevantTransactions.haveClosed() )
            {
                // At least one transaction hasn't closed yet
                return false;
            }

            // This transaction has been closed, unlink so we don't have to check it the next time
            relevantTransactions = relevantTransactions.next;
        }

        // All transactions have been closed
        return true;
    }

    private static class Tx
    {
        private final KernelTransactionStamp txStamp;
        private Tx next;

        Tx( KernelTransactionStamp tx )
        {
            this.txStamp = tx;
        }

        boolean haveClosed()
        {
            return !txStamp.isOpen();
        }
    }
}
