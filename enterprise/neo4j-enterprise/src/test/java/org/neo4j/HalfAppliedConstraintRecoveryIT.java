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
package org.neo4j;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.concurrent.OtherThreadRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
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
    private static final String KEY2 = "key2";
    private static final Consumer<GraphDatabaseAPI> UNIQUE_CONSTRAINT_CREATOR =
            db -> db.schema().constraintFor( LABEL ).assertPropertyIsUnique( KEY ).create();

    private static final Consumer<GraphDatabaseAPI> NODE_KEY_CONSTRAINT_CREATOR =
            db -> db.execute( "CREATE CONSTRAINT ON (n:" + LABEL.name() + ") ASSERT (n." + KEY + ") IS NODE KEY" );

    private static final Consumer<GraphDatabaseAPI> COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR =
            db -> db.execute( "CREATE CONSTRAINT ON (n:" + LABEL.name() + ") ASSERT (n." + KEY + ", n." + KEY2 + ") IS NODE KEY" );
    private static final BiConsumer<GraphDatabaseAPI,List<TransactionRepresentation>> REAPPLY =
            ( db, txs ) -> apply( db, txs.subList( txs.size() - 1, txs.size() ) );

    private static BiConsumer<GraphDatabaseAPI,List<TransactionRepresentation>> recreate( Consumer<GraphDatabaseAPI> constraintCreator )
    {
        return ( db, txs ) -> createConstraint( db, constraintCreator );
    }

    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( "T2" );
    private final Monitors monitors = new Monitors();

    @Test
    public void recoverFromAndContinueApplyHalfConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( REAPPLY, UNIQUE_CONSTRAINT_CREATOR, false );
    }

    @Test
    public void recoverFromAndRecreateHalfConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( recreate( UNIQUE_CONSTRAINT_CREATOR ), UNIQUE_CONSTRAINT_CREATOR, false );
    }

    @Test
    public void recoverFromAndContinueApplyHalfNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( REAPPLY, NODE_KEY_CONSTRAINT_CREATOR, false );
    }

    @Test
    public void recoverFromAndRecreateHalfNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( recreate( NODE_KEY_CONSTRAINT_CREATOR ), NODE_KEY_CONSTRAINT_CREATOR, false );
    }

    @Test
    public void recoverFromAndContinueApplyHalfCompositeNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( REAPPLY, COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR, true );
    }

    @Test
    public void recoverFromAndRecreateHalfCompositeNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        recoverFromHalfConstraintAppliedBeforeCrash( recreate( COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR ), COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR, true );
    }

    private void recoverFromHalfConstraintAppliedBeforeCrash( BiConsumer<GraphDatabaseAPI,List<TransactionRepresentation>> applier,
            Consumer<GraphDatabaseAPI> constraintCreator, boolean composite ) throws Exception
    {
        // GIVEN
        List<TransactionRepresentation> transactions = createTransactionsForCreatingConstraint( constraintCreator );
        GraphDatabaseAPI db = newDb();
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
        db = (GraphDatabaseAPI) new TestEnterpriseGraphDatabaseFactory().setFileSystem( crashSnapshot ).newImpermanentDatabase();
        try
        {
            applier.accept( db, transactions );

            // THEN
            try ( Transaction tx = db.beginTx() )
            {
                ConstraintDefinition constraint = single( db.schema().getConstraints( LABEL ) );
                assertEquals( LABEL.name(), constraint.getLabel().name() );
                if ( composite )
                {
                    assertEquals( Arrays.asList( KEY, KEY2 ), Iterables.asList( constraint.getPropertyKeys() ) );
                }
                else
                {
                    assertEquals( KEY, single( constraint.getPropertyKeys() ) );
                }
                IndexDefinition index = single( db.schema().getIndexes( LABEL ) );
                assertEquals( LABEL.name(), index.getLabel().name() );
                if ( composite )
                {
                    assertEquals( Arrays.asList( KEY, KEY2 ), Iterables.asList( index.getPropertyKeys() ) );
                }
                else
                {
                    assertEquals( KEY, single( index.getPropertyKeys() ) );
                }
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void recoverFromNonUniqueHalfConstraintAppliedBeforeCrash() throws Exception
    {
        // GIVEN
        recoverFromConstraintAppliedBeforeCrash( UNIQUE_CONSTRAINT_CREATOR );
    }

    @Test
    public void recoverFromNonUniqueHalfNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        // GIVEN
        recoverFromConstraintAppliedBeforeCrash( NODE_KEY_CONSTRAINT_CREATOR );
    }

    @Test
    public void recoverFromNonUniqueHalfCompositeNodeKeyConstraintAppliedBeforeCrash() throws Exception
    {
        // GIVEN
        recoverFromConstraintAppliedBeforeCrash( COMPOSITE_NODE_KEY_CONSTRAINT_CREATOR );
    }

    private void recoverFromConstraintAppliedBeforeCrash( Consumer<GraphDatabaseAPI> constraintCreator ) throws Exception
    {
        List<TransactionRepresentation> transactions = createTransactionsForCreatingConstraint( constraintCreator );
        EphemeralFileSystemAbstraction crashSnapshot;
        {
            GraphDatabaseAPI db = newDb();
            Barrier.Control barrier = new Barrier.Control();
            monitors.addMonitorListener( new IndexingService.MonitorAdapter()
            {
                @Override
                public void indexPopulationScanComplete()
                {
                    barrier.reached();
                }
            } );
            try
            {
                // Create two nodes that have duplicate property values
                String value = "v";
                try ( Transaction tx = db.beginTx() )
                {
                    for ( int i = 0; i < 2; i++ )
                    {
                        db.createNode( LABEL ).setProperty( KEY, value );
                    }
                    tx.success();
                }
                t2.execute( state ->
                {
                    apply( db, transactions.subList( 0, transactions.size() - 1 ) );
                    return null;
                } );
                barrier.await();
                flushStores( db );
                // Crash before index population have discovered that there are duplicates
                // (nowadays happens in between index population and creating the constraint)
                crashSnapshot = fs.snapshot();
                barrier.release();
            }
            finally
            {
                db.shutdown();
            }
        }

        // WHEN
        {
            GraphDatabaseAPI db = (GraphDatabaseAPI) new TestEnterpriseGraphDatabaseFactory().setFileSystem( crashSnapshot )
                    .newImpermanentDatabase();
            try
            {
                recreate( constraintCreator ).accept( db, transactions );
                fail( "Should not be able to create constraint on non-unique data" );
            }
            catch ( ConstraintViolationException | QueryExecutionException e )
            {
                // THEN good
            }
            finally
            {
                db.shutdown();
            }
        }
    }

    private GraphDatabaseAPI newDb()
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( fs ).setMonitors( monitors )
                .newImpermanentDatabase();
    }

    private static void flushStores( GraphDatabaseAPI db )
    {
        db.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().flush( IOLimiter.unlimited() );
    }

    private static void apply( GraphDatabaseAPI db, List<TransactionRepresentation> transactions )
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

    private static List<TransactionRepresentation> createTransactionsForCreatingConstraint( Consumer<GraphDatabaseAPI> uniqueConstraintCreator )
            throws Exception
    {
        // A separate db altogether
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            createConstraint( db, uniqueConstraintCreator );
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

    private static void createConstraint( GraphDatabaseAPI db, Consumer<GraphDatabaseAPI> constraintCreator )
    {
        try ( Transaction tx = db.beginTx() )
        {
            constraintCreator.accept( db );
            tx.success();
        }
    }
}
