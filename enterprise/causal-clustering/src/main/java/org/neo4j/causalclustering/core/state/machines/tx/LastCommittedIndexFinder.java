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
package org.neo4j.causalclustering.core.state.machines.tx;

import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.decodeLogIndexFromTxHeader;

/**
 * Finds the last committed transaction in the transaction log, then decodes the header as a raft index.
 * This allows us to correlate raft log with transaction log on recovery.
 */
public class LastCommittedIndexFinder
{
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore transactionStore;
    private final Log log;

    public LastCommittedIndexFinder( TransactionIdStore transactionIdStore,
                                     LogicalTransactionStore transactionStore, LogProvider logProvider )
    {
        this.transactionIdStore = transactionIdStore;
        this.transactionStore = transactionStore;
        this.log = logProvider.getLog( getClass() );
    }

    public long getLastCommittedIndex()
    {
        long lastConsensusIndex;
        long lastTxId = transactionIdStore.getLastCommittedTransactionId();
        log.info( "Last transaction id in metadata store %d", lastTxId );

        CommittedTransactionRepresentation lastTx = null;
        try ( IOCursor<CommittedTransactionRepresentation> transactions =
                transactionStore.getTransactions( lastTxId ) )
        {
            while ( transactions.next() )
            {
                lastTx = transactions.get();
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }

        if ( lastTx == null )
        {
            throw new RuntimeException( "We must have at least one transaction telling us where we are at in the consensus log." );
        }

        log.info( "Start id of last committed transaction in transaction log %d", lastTx.getStartEntry().getLastCommittedTxWhenTransactionStarted() );
        log.info( "Last committed transaction id in transaction log %d", lastTx.getCommitEntry().getTxId() );

        byte[] lastHeaderFound = lastTx.getStartEntry().getAdditionalHeader();
        lastConsensusIndex = decodeLogIndexFromTxHeader( lastHeaderFound );

        log.info( "Last committed consensus log index committed into tx log %d", lastConsensusIndex );
        return lastConsensusIndex;
    }
}
