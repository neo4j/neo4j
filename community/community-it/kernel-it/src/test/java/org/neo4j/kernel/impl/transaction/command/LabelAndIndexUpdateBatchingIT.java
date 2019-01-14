/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * This test is for an issue with transaction batching where there would be a batch of transactions
 * to be applied in the same batch; the batch containing a creation of node N with label L and property P.
 * Later in that batch there would be a uniqueness constraint created for label L and property P.
 * The number of nodes matching this constraint would be few and so the label scan store would be selected
 * to drive the population of the index. Problem is that the label update for N would still sit in
 * the batch state, to be applied at the end of the batch. Hence the node would be forgotten when the
 * index was being built.
 */
public class LabelAndIndexUpdateBatchingIT
{
    private static final String PROPERTY_KEY = "key";
    private static final Label LABEL = Label.label( "label" );

    @Test
    public void indexShouldIncludeNodesCreatedPreviouslyInBatch() throws Exception
    {
        // GIVEN a transaction stream leading up to this issue
        // perform the transactions from db-level and extract the transactions as commands
        // so that they can be applied batch-wise they way we'd like to later.

        // a bunch of nodes (to have the index population later on to decide to use label scan for population)
        List<TransactionRepresentation> transactions;
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        String nodeN = "our guy";
        String otherNode = "just to create the tokens";
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( LABEL ).setProperty( PROPERTY_KEY, otherNode );
                for ( int i = 0; i < 10_000; i++ )
                {
                    db.createNode();
                }
                tx.success();
            }
            // node N
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( LABEL ).setProperty( PROPERTY_KEY, nodeN );
                tx.success();
            }
            // uniqueness constraint affecting N
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
                tx.success();
            }
            transactions = extractTransactions( db );
        }
        finally
        {
            db.shutdown();
        }

        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        TransactionCommitProcess commitProcess =
                db.getDependencyResolver().resolveDependency( TransactionCommitProcess.class );
        try
        {
            int cutoffIndex = findCutoffIndex( transactions );
            commitProcess.commit( toApply( transactions.subList( 0, cutoffIndex ) ), NULL, EXTERNAL );

            // WHEN applying the two transactions (node N and the constraint) in the same batch
            commitProcess.commit( toApply( transactions.subList( cutoffIndex, transactions.size() ) ), NULL, EXTERNAL );

            // THEN node N should've ended up in the index too
            try ( Transaction tx = db.beginTx() )
            {
                assertNotNull( "Verification node not found",
                        singleOrNull( db.findNodes( LABEL, PROPERTY_KEY, otherNode ) ) ); // just to verify
                assertNotNull( "Node N not found",
                        singleOrNull( db.findNodes( LABEL, PROPERTY_KEY, nodeN ) ) );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }

    }

    private static int findCutoffIndex( Collection<TransactionRepresentation> transactions ) throws IOException
    {
        Iterator<TransactionRepresentation> iterator = transactions.iterator();
        for ( int i = 0; iterator.hasNext(); i++ )
        {
            TransactionRepresentation tx = iterator.next();
            CommandExtractor extractor = new CommandExtractor();
            tx.accept( extractor );
            List<StorageCommand> commands = extractor.getCommands();
            List<StorageCommand> nodeCommands = commands.stream()
                    .filter( command -> command instanceof NodeCommand ).collect( toList() );
            if ( nodeCommands.size() == 1 )
            {
                return i;
            }
        }
        throw new AssertionError( "Couldn't find the transaction which would be the cut-off point" );
    }

    private static TransactionToApply toApply( Collection<TransactionRepresentation> transactions )
    {
        TransactionToApply first = null;
        TransactionToApply last = null;
        for ( TransactionRepresentation transactionRepresentation : transactions )
        {
            TransactionToApply transaction = new TransactionToApply( transactionRepresentation );
            if ( first == null )
            {
                first = last = transaction;
            }
            else
            {
                last.next( transaction );
                last = transaction;
            }
        }
        return first;
    }

    private static List<TransactionRepresentation> extractTransactions( GraphDatabaseAPI db )
            throws IOException
    {
        LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
        List<TransactionRepresentation> transactions = new ArrayList<>();
        try ( TransactionCursor cursor = txStore.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
        {
            cursor.forAll( tx -> transactions.add( tx.getTransactionRepresentation() ) );
        }
        return transactions;
    }
}
