/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.constraints;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * It's master creating a constraint. There are two mini transactions in creating a constraint:
 * <ol>
 * <li>Create the backing index and activating the constraint (index population follows).</li>
 * <li>Activating the constraint after successful index population.</li>
 * </ol>
 *
 * If slave pulls the first mini transaction, but crashes or otherwise does a nonclean shutdown before it gets
 * the other mini transaction (and that index record happens to have been evicted to disk in between)
 * then the next start of that slave would set that constraint index as failed and even delete it
 * and refuse to activate it when it eventually would pull the other mini transaction which wanted to
 * activate the constraint.
 *
 * This issue is tested in single db mode because it's way easier to reliably test in this environment.
 */
public class HalfAppliedConstraintRecoveryIT
{
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";

    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldBeAbleToRecoverFromAndContinueApplyHalfConstraintAppliedBeforeCrash() throws Exception
    {
        shouldBeAbleToRecoverFromHalfConstraintAppliedBeforeCrash( true );
    }

    @Test
    public void shouldBeAbleToRecoverFromAndRecreateHalfConstraintAppliedBeforeCrash() throws Exception
    {
        shouldBeAbleToRecoverFromHalfConstraintAppliedBeforeCrash( false );
    }

    private void shouldBeAbleToRecoverFromHalfConstraintAppliedBeforeCrash( boolean reApply ) throws Exception
    {
        // GIVEN
        List<TransactionRepresentation> transactions = createTransactionsForCreatingConstraint();
        GraphDatabaseAPI db =
                (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase();
        EphemeralFileSystemAbstraction crashSnapshot;
        try
        {
            apply( db, transactions.subList( 0, transactions.size() - 1 ) );
            flushStores( db );
            crashSnapshot = fs.snapshot();
        }
        finally
        {
            db.shutdown();
        }

        // WHEN
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( crashSnapshot ).newImpermanentDatabase();
        try
        {
            if ( reApply )
            {
                // Continue to apply this transaction, as if the rest of the creation was pulled
                apply( db, transactions.subList( transactions.size() - 1, transactions.size() ) );
            }
            else
            {
                // Make another whole attempt to create this contraint, where this index should just be reused.
                createConstraint( db );
            }

            // THEN
            try ( Transaction tx = db.beginTx() )
            {
                ConstraintDefinition constraint = single( db.schema().getConstraints( LABEL ) );
                assertEquals( LABEL.name(), constraint.getLabel().name() );
                assertEquals( KEY, single( constraint.getPropertyKeys() ) );
                IndexDefinition index = single( db.schema().getIndexes( LABEL ) );
                assertEquals( LABEL.name(), index.getLabel().name() );
                assertEquals( KEY, single( index.getPropertyKeys() ) );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private void flushStores( GraphDatabaseAPI db )
    {
        db.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().flush( IOLimiter.unlimited() );
    }

    private void apply( GraphDatabaseAPI db, List<TransactionRepresentation> transactions )
    {
        TransactionCommitProcess committer =
                db.getDependencyResolver().resolveDependency( TransactionCommitProcess.class );
        transactions.forEach( tx ->
        {
            try
            {
                committer.commit( new TransactionToApply( tx ), CommitEvent.NULL, EXTERNAL );
            }
            catch ( TransactionFailureException e )
            {
                throw new RuntimeException( e );
            }
        } );
    }

    private List<TransactionRepresentation> createTransactionsForCreatingConstraint() throws Exception
    {
        // A separate db altogether
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            createConstraint( db );
            LogicalTransactionStore txStore =
                    db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
            List<TransactionRepresentation> transactions = new ArrayList<>();
            try ( TransactionCursor cursor = txStore.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
            {
                cursor.forAll( tx -> transactions.add( tx.getTransactionRepresentation() ) );
            }
            return transactions;
        }
        finally
        {
            db.shutdown();
        }
    }

    private void createConstraint( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( LABEL ).assertPropertyIsUnique( KEY ).create();
            tx.success();
        }
    }
}
