/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.tools.applytx;

import java.io.PrintStream;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.api.TransactionQueue;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.tools.console.input.ArgsCommand;

import static java.lang.String.format;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;

/**
 * Re-applies transactions found in the store onto the store AGAIN, w/o appending to transaction log.
 * This has a special use case of trying out transaction application logic and when the entire transaction history isn't available.
 */
public class ReapplyTransactionsCommand extends ArgsCommand
{
    private final Supplier<GraphDatabaseAPI> to;

    public ReapplyTransactionsCommand( Supplier<GraphDatabaseAPI> to )
    {
        this.to = to;
    }

    @Override
    protected void run( Args args, PrintStream out ) throws Exception
    {
        DependencyResolver dependencyResolver = to.get().getDependencyResolver();
        if ( args.get( "from" ) == null )
        {
            throw new IllegalArgumentException( "No tx range specified, please specify at least -from. -to is optional" );
        }

        long from = args.getNumber( "from", -1 ).longValue();
        long to = args.getNumber( "to", -1 ).longValue();
        int batchSize = args.getNumber( "batchSize", 1 ).intValue();

        // Do the re-apply
        LogicalTransactionStore txStore = dependencyResolver.resolveDependency( LogicalTransactionStore.class );
        if ( to == -1 )
        {
            to = dependencyResolver.resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
        }
        StorageEngine storageEngine = dependencyResolver.resolveDependency( StorageEngine.class );
        long totalCount = to - from + 1;
        ProgressListener progress = ProgressMonitorFactory.textual( out ).singlePart( "Re-apply " + from + "-" + to + " (" + totalCount + " txs)", totalCount );
        try ( TransactionCursor transactions = txStore.getTransactions( from ) )
        {
            TransactionQueue batch = new TransactionQueue( batchSize,
                    // This is only applying to the store, not commiting with appending to the log and all
                    ( first, last ) -> storageEngine.apply( first, RECOVERY ) );
            while ( transactions.next() )
            {
                CommittedTransactionRepresentation tx = transactions.get();
                TransactionToApply txToApply = new TransactionToApply( tx.getTransactionRepresentation() );
                txToApply.commitment( Commitment.NO_COMMITMENT, tx.getCommitEntry().getTxId() );
                batch.queue( txToApply );
                progress.add( 1 );
                if ( to != -1 && tx.getCommitEntry().getTxId() >= to )
                {
                    break;
                }
            }
            batch.empty();
        }

        out.println( "Re-applied transactions " + from + "-" + to );
    }

    @Override
    public String toString()
    {
        return ArrayUtil.join( new String[] {
                "Re-applies transactions onto the db. Applied transactions won't be appended to the transaction log, only applied onto the store. Example:",
                "  -from 134  : re-applies transactions 134 up to last committed transaction id onto the store",
                "  -from 134 -to 256 : re-applies transactions 134-256 (inclusive) onto the store" },
                format( "%n" ) );
    }
}
