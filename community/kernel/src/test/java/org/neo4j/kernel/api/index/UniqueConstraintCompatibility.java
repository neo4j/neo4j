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
package org.neo4j.kernel.api.index;

import java.io.File;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Consumer;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import static org.neo4j.kernel.impl.locking.LockService.LockType;

@Ignore( "Not a test. This is a compatibility suite that provides test cases for verifying" +
        " SchemaIndexProvider implementations. Each index provider that is to be tested by this suite" +
        " must create their own test class extending IndexProviderCompatibilityTestSuite." +
        " The @Ignore annotation doesn't prevent these tests to run, it rather removes some annoying" +
        " errors or warnings in some IDEs about test classes needing a public zero-arg constructor." )
public class UniqueConstraintCompatibility extends IndexProviderCompatibilityTestSuite.Compatibility
{
    public UniqueConstraintCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite );
    }

    /*
     * There are a quite a number of permutations to consider, when it comes to unique
     * constraints.
     *
     * We have two supported providers:
     *  - InMemoryIndexProvider
     *  - LuceneSchemaIndexProvider
     *
     * An index can be in a number of states, two of which are interesting:
     *  - ONLINE: the index is in active duty
     *  - POPULATING: the index is in the process of being created and filled with data
     *
     * Further more, indexes that are POPULATING have two ways of ingesting data:
     *  - Through add()'ing existing data
     *  - Through NodePropertyUpdates sent to a "populating udpater"
     *
     * Then, when we add data to an index, two outcomes are possible, depending on the
     * data:
     *  - The index does not contain an equivalent value, and the entity id is added to
     *    the index.
     *  - The index already contains an equivalent value, and the addition is rejected.
     *
     * And when it comes to observing these outcomes, there are a whole bunch of
     * interesting transaction states that are worth exploring:
     *  - Adding a label to a node
     *  - Removing a label from a node
     *  - Combinations of adding and removing a label, ultimately adding it
     *  - Combinations of adding and removing a label, ultimately removing it
     *  - Adding a property
     *  - Removing a property
     *  - Changing an existing property
     *  - Combinations of adding and removing a property, ultimately adding it
     *  - Combinations of adding and removing a property, ultimately removing it
     *  - Likewise combinations of adding, removing and changing a property
     *
     * To make matters worse, we index a number of different types, some of which may or
     * may not collide in the index because of coercion. We need to make sure that the
     * indexes deal with these values correctly. And we also have the ways in which these
     * operations can be performed in any number of transactions, for instance, if all
     * the conflicting nodes were added in the same transaction or not.
     *
     * All in all, we have many cases to test for!
     *
     * Still, it is possible to boild things down a little bit, because there are fewer
     * outcomes than there are scenarios that lead to those outcomes. With a bit of
     * luck, we can abstract over the scenarios that lead to those outcomes, and then
     * only write a test per outcome. These are the outcomes I see:
     *  - Populating an index succeeds
     *  - Populating an index fails because of the existing data
     *  - Populating an index fails because of updates to data
     *  - Adding to an online index succeeds
     *  - Adding to an online index fails because of existing data
     *  - Adding to an online index fails because of data in the same transaction
     *
     * There's a lot of work to be done here.
     */

    // -- Tests:

    @Test
    public void onlineConstraintShouldAcceptDistinctValuesInDifferentTransactions()
    {
        // Given
        givenOnlineConstraint();

        // When
        Node n;
        try ( Transaction tx = db.beginTx() )
        {
            n = db.createNode( label );
            n.setProperty( property, "n" );
            tx.success();
        }

        // Then
        transaction(
                assertLookupNode( "a", is( a ) ),
                assertLookupNode( "n" , is( n ) ) );
    }

    @Test
    public void onlineConstraintShouldAcceptDistinctValuesInSameTransaction()
    {
        // Given
        givenOnlineConstraint();

        // When
        Node n, m;
        try ( Transaction tx = db.beginTx() )
        {
            n = db.createNode( label );
            n.setProperty( property, "n" );

            m = db.createNode( label );
            m.setProperty( property, "m" );
            tx.success();
        }

        // Then
        transaction(
                assertLookupNode( "n", is( n ) ),
                assertLookupNode( "m", is( m ) ) );
    }

    @Ignore("Until constraint violation has been updated to double check with property store")
    @Test
    public void onlineConstrainthouldNotFalselyCollideOnFindNodesByLabelAndProperty() throws Exception
    {
        // Given
        givenOnlineConstraint();
        Node n, m;
        try ( Transaction tx = db.beginTx() )
        {
            n = db.createNode( label );
            n.setProperty( property, COLLISION_X );
            tx.success();
        }

        // When
        try ( Transaction tx = db.beginTx() )
        {
            m = db.createNode( label );
            m.setProperty( property, COLLISION_Y );
            tx.success();
        }

        // Then
        transaction(
                assertLookupNode( COLLISION_X, is( n ) ),
                assertLookupNode( COLLISION_Y, is( m ) ) );
    }

    // Replaces UniqueIAC: shouldConsiderWholeTransactionForValidatingUniqueness
    @Test
    public void onlineConstraintShouldNotConflictOnIntermediateStatesInSameTransaction()
    {
        // Given
        givenOnlineConstraint();

        // When
        transaction(
                setProperty( a, "b" ),
                setProperty( b, "a" ),
                success );

        // Then
        transaction(
                assertLookupNode( "a", is( b ) ),
                assertLookupNode( "b", is( a ) ) );
    }

    // Replaces UniqueIAC: shouldRejectChangingEntryToAlreadyIndexedValue
    @Test( expected = ConstraintViolationException.class )
    public void onlineConstraintShouldRejectChangingEntryToAlreadyIndexedValue()
    {
        // Given
        givenOnlineConstraint();
        transaction(
                setProperty( b, "b" ),
                success );

        // When
        transaction(
                setProperty( b, "a" ),
                success,
                fail( "Changing a property to an already indexed value should have thrown" ) );
    }

    // Replaces UniqueIPC: should*EnforceUnqieConstraintsAgainstDataAddedInSameTx
    @Test( expected = ConstraintViolationException.class)
    public void onlineConstraintShouldRejectConflictsInTheSameTransaction() throws Exception
    {
        // Given
        givenOnlineConstraint();

        // Then
        transaction(
                setProperty( a, "x" ),
                setProperty( b, "x" ),
                success,
                fail( "Should have rejected changes of two node/properties to the same index value" ) );
    }

    @Test
    public void onlineConstraintShouldRejectChangingEntryToAlreadyIndexedValueThatOtherTransactionsAreRemoving()
            throws Exception
    {
        // Given
        givenOnlineConstraint();
        transaction(
                setProperty( b, "b" ),
                success );

        Transaction otherTx = db.beginTx();
        a.removeLabel( label );
        suspend( otherTx );

        // When
        try
        {
            transaction(
                    setProperty( b, "a" ),
                    success,
                    fail( "Changing a property to an already indexed value should have thrown" ) );
        }
        catch ( ConstraintViolationException ignore )
        {
            // we're happy
        }
        finally
        {
            resume( otherTx );
            otherTx.failure();
            otherTx.close();
        }
    }

    // Replaces UniqueIAC: shouldRemoveAndAddEntries
    @Test
    public void onlineConstraintShouldAddAndRemoveFromIndexAsPropertiesAndLabelsChange()
    {
        // Given
        givenOnlineConstraint();

        // When
        transaction( setProperty( b, "b" ), success );
        transaction( setProperty( c, "c" ), addLabel( c, label ), success );
        transaction( setProperty( d, "d" ), addLabel( d, label ), success );
        transaction( removeProperty( a ), success );
        transaction( removeProperty( b ), success );
        transaction( removeProperty( c ), success );
        transaction( setProperty( a, "a" ), success );
        transaction( setProperty( c, "c2" ), success );

        // Then
        transaction(
                assertLookupNode( "a", is( a ) ),
                assertLookupNode( "b", is( nullValue( Node.class ) ) ),
                assertLookupNode( "c", is( nullValue( Node.class ) ) ),
                assertLookupNode( "d", is( d ) ),
                assertLookupNode( "c2", is( c ) ) );
    }

    // Replaces UniqueIAC: shouldRejectEntryWithAlreadyIndexedValue
    @Test( expected = ConstraintViolationException.class )
    public void onlineConstraintShouldRejectConflictingPropertyChange()
    {
        // Given
        givenOnlineConstraint();

        // Then
        transaction(
                setProperty( b, "a" ),
                success,
                fail( "Setting b.name = \"a\" should have caused a conflict" ) );
    }

    @Test( expected = ConstraintViolationException.class )
    public void onlineConstraintShouldRejectConflictingLabelChange()
    {
        // Given
        givenOnlineConstraint();

        // Then
        transaction(
                addLabel( c, label ),
                success,
                fail( "Setting c:Cybermen should have caused a conflict" ) );
    }

    // Replaces UniqueIAC: shouldRejectAddingEntryToValueAlreadyIndexedByPriorChange
    @Test( expected = ConstraintViolationException.class )
    public void onlineConstraintShouldRejectAddingEntryForValueAlreadyIndexedByPriorChange()
    {
        // Given
        givenOnlineConstraint();

        // When
        transaction( setProperty( a, "a1" ), success ); // This is a CHANGE update

        // Then
        transaction(
                setProperty( b, "a1" ),
                success,
                fail( "Setting b.name = \"a1\" should have caused a conflict" ) );
    }

    // Replaces UniqueIAC: shouldAddUniqueEntries
    // Replaces UniqueIPC: should*EnforceUniqueConstraintsAgainstDataAddedOnline
    @Test
    public void onlineConstraintShouldAcceptUniqueEntries()
    {
        // Given
        givenOnlineConstraint();

        // When
        transaction( setProperty( b, "b" ), addLabel( d, label ), success );
        transaction( setProperty( c, "c" ), addLabel( c, label ), success );

        // Then
        transaction(
                assertLookupNode( "a", is( a ) ),
                assertLookupNode( "b", is( b ) ),
                assertLookupNode( "c", is( c ) ),
                assertLookupNode( "d", is( d ) ) );
    }

    // Replaces UniqueIAC: shouldUpdateUniqueEntries
    @Test
    public void onlineConstraintShouldAcceptUniqueEntryChanges()
    {
        // Given
        givenOnlineConstraint();

        // When
        transaction( setProperty( a, "a1" ), success ); // This is a CHANGE update

        // Then
        transaction( assertLookupNode( "a1", is( a ) ) );
    }

    // Replaces UniqueIAC: shoouldRejectEntriesInSameTransactionWithDuplicateIndexedValue\
    @Test( expected = ConstraintViolationException.class )
    public void onlineConstraintShouldRejectDuplicateEntriesAddedInSameTransaction()
    {
        // Given
        givenOnlineConstraint();

        // Then
        transaction(
                setProperty( b, "d" ),
                addLabel( d, label ),
                success,
                fail( "Setting b.name = \"d\" and d:Cybermen should have caused a conflict" ));
    }

    // Replaces UniqueIPC: should*EnforceUniqueConstraints
    // Replaces UniqueIPC: should*EnforceUniqueConstraintsAgainstDataAddedThroughPopulator
    @Test
    public void populatingConstraintMustAcceptDatasetOfUniqueEntries()
    {
        // Given
        givenUniqueDataset();

        // Then this does not throw:
        createUniqueConstraint();
    }

    @Test( expected = ConstraintViolationException.class )
    public void populatingConstraintMustRejectDatasetWithDuplicateEntries()
    {
        // Given
        givenUniqueDataset();
        transaction(
                setProperty( c, "b" ), // same property value as 'b' has
                success );

        // Then this must throw:
        createUniqueConstraint();
    }

    @Test
    public void populatingConstraintMustAcceptDatasetWithDalseIndexCollisions()
    {
        // Given
        givenUniqueDataset();
        transaction(
                setProperty( b, COLLISION_X ),
                setProperty( c, COLLISION_Y ),
                success );

        // Then this does not throw:
        createUniqueConstraint();
    }

    @Test
    public void populatingConstraintMustAcceptDatasetThatGetsUpdatedWithUniqueEntries() throws Exception
    {
        // Given
        givenUniqueDataset();

        // When
        Future<?> createConstraintTransaction = applyChangesToPopulatingUpdater(
                d.getId(), a.getId(), setProperty( d, "d1" ) );

        // Then observe that our constraint was created successfully:
        createConstraintTransaction.get();
        // Future.get() will throw an ExecutionException, if the Runnable threw an exception.
    }

    // Replaces UniqueLucIAT: shouldRejectEntryWithAlreadyIndexedValue
    @Test
    public void populatingConstraintMustRejectDatasetThatGetsUpdatedWithDuplicateAddition() throws Exception
    {
        // Given
        givenUniqueDataset();

        // When
        Future<?> createConstraintTransaction = applyChangesToPopulatingUpdater(
                d.getId(), a.getId(), createNode( "b" ) );

        // Then observe that our constraint creation failed:
        try
        {
            createConstraintTransaction.get();
            Assert.fail( "expected to throw when PopulatingUpdater got duplicates" );
        }
        catch ( ExecutionException ee )
        {
            Throwable cause = ee.getCause();
            assertThat( cause, instanceOf( ConstraintViolationException.class ) );
        }
    }

    // Replaces UniqueLucIAT: shouldRejectChangingEntryToAlreadyIndexedValue
    @Test
    public void populatingConstraintMustRejectDatasetThatGetsUpdatedWithDuplicates() throws Exception
    {
        // Given
        givenUniqueDataset();

        // When
        Future<?> createConstraintTransaction = applyChangesToPopulatingUpdater(
                d.getId(), a.getId(), setProperty( d, "b" ) );

        // Then observe that our constraint creation failed:
        try
        {
            createConstraintTransaction.get();
            Assert.fail( "expected to throw when PopulatingUpdater got duplicates" );
        }
        catch ( ExecutionException ee )
        {
            Throwable cause = ee.getCause();
            assertThat( cause, instanceOf( ConstraintViolationException.class ) );
        }
    }

    @Test
    public void populatingConstraintMustAcceptDatasetThatGestUpdatedWithFalseIndexCollisions() throws Exception
    {
        // Given
        givenUniqueDataset();
        transaction( setProperty( a, COLLISION_X ), success );

        // When
        Future<?> createConstraintTransaction = applyChangesToPopulatingUpdater(
                d.getId(), a.getId(), setProperty( d, COLLISION_Y ) );

        // Then observe that our constraint was created successfully:
        createConstraintTransaction.get();
        // Future.get() will throw an ExecutionException, if the Runnable threw an exception.
    }

    // Replaces UniqueLucIAT: shouldRejectEntriesInSameTransactionWithDuplicatedIndexedValues
    @Test
    public void populatingConstraintMustRejectDatasetThatGetsUpdatedWithDuplicatesInSameTransaction() throws Exception
    {
        // Given
        givenUniqueDataset();

        // When
        Future<?> createConstraintTransaction = applyChangesToPopulatingUpdater(
                d.getId(), a.getId(), setProperty( d, "x" ), setProperty( c, "x" ) );

        // Then observe that our constraint creation failed:
        try
        {
            createConstraintTransaction.get();
            Assert.fail( "expected to throw when PopulatingUpdater got duplicates" );
        }
        catch ( ExecutionException ee )
        {
            Throwable cause = ee.getCause();
            assertThat( cause, instanceOf( ConstraintViolationException.class ) );
        }
    }

    @Test
    public void populatingConstraintMustAcceptDatasetThatGetsUpdatedWithDuplicatesThatAreLaterResolved() throws Exception
    {
        // Given
        givenUniqueDataset();

        // When
        Future<?> createConstraintTransaction = applyChangesToPopulatingUpdater(
                d.getId(),
                a.getId(),
                setProperty( d, "b" ), // Cannot touch node 'a' because that one is locked
                setProperty( b, "c" ),
                setProperty( c, "d" ) );

        // Then observe that our constraint was created successfully:
        createConstraintTransaction.get();
        // Future.get() will throw an ExecutionException, if the Runnable threw an exception.
    }

    // Replaces UniqueLucIAT: shouldRejectAddingEntryToValueAlreadyIndexedByPriorChange
    @Test
    public void populatingUpdaterMustRejectDatasetWhereAdditionsConflictsWithPriorChanges() throws Exception
    {
        // Given
        givenUniqueDataset();

        // When
        Future<?> createConstraintTransaction = applyChangesToPopulatingUpdater(
                d.getId(), a.getId(), setProperty( d, "x" ), createNode( "x" ) );

        // Then observe that our constraint creation failed:
        try
        {
            createConstraintTransaction.get();
            Assert.fail( "expected to throw when PopulatingUpdater got duplicates" );
        }
        catch ( ExecutionException ee )
        {
            Throwable cause = ee.getCause();
            assertThat( cause, instanceOf( ConstraintViolationException.class ) );
        }
    }

    /**
     * NOTE the tests using this will currently succeed for the wrong reasons,
     * because the data-changing transaction does not actually release the
     * schema read lock early enough for the PopulatingUpdater to come into
     * play.
     */
    private Future<?> applyChangesToPopulatingUpdater(
            long blockDataChangeTransactionOnLockOnId,
            long blockPopulatorOnLockOnId,
            final Action... actions ) throws InterruptedException, ExecutionException
    {
        // We want to issue an update to an index populator for a constraint.
        // However, creating a constraint takes a schema write lock, while
        // creating nodes and setting their properties takes a schema read
        // lock. We need to sneak past these locks.
        final CountDownLatch createNodeReadyLatch = new CountDownLatch( 1 );
        final CountDownLatch createNodeCommitLatch = new CountDownLatch( 1 );
        Future<?> updatingTransaction = executor.submit( new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = db.beginTx() )
                {
                    for ( Action action : actions )
                    {
                        action.accept( tx );
                    }
                    tx.success();
                    createNodeReadyLatch.countDown();
                    awaitUninterruptibly( createNodeCommitLatch );
                }
            }
        } );
        createNodeReadyLatch.await();

        // The above transaction now contain the changes we want to expose to
        // the IndexUpdater as updates. This will happen when we commit the
        // transaction. The transaction now also holds the schema read lock,
        // so we can't begin creating our constraint just yet.
        // We first have to unlock the schema, and then block just before we
        // send off our updates. We can do that by making another thread take a
        // read lock on the node we just created, and then initiate our commit.
        Lock lockBlockingDataChangeTransaction = getLockService().acquireNodeLock(
                blockDataChangeTransactionOnLockOnId,
                LockType.WRITE_LOCK );

        // Before we begin creating the constraint, we take a write lock on an
        // "earlier" node, to hold up the populator for the constraint index.
        Lock lockBlockingIndexPopulator = getLockService().acquireNodeLock(
                blockPopulatorOnLockOnId,
                LockType.WRITE_LOCK );

        // This thread tries to create a constraint. It should block, waiting for it's
        // population job to finish, and it's population job should in turn be blocked
        // on the lockBlockingIndexPopulator above:
        final CountDownLatch createConstraintTransactionStarted = new CountDownLatch( 1 );
        Future<?> createConstraintTransaction = executor.submit( new Runnable()
        {
            @Override
            public void run()
            {

                createUniqueConstraint( createConstraintTransactionStarted );
            }
        } );
        createConstraintTransactionStarted.await();

        // Now we can initiate the data-changing commit. It should then
        // release the schema read lock, and block on the
        // lockBlockingDataChangeTransaction.
        createNodeCommitLatch.countDown();

        // Now we can issue updates to the populator in the still ongoing population job.
        // We do that by releasing the lock that is currently preventing our
        // data-changing transaction from committing.
        lockBlockingDataChangeTransaction.release();

        // And we observe that our updating transaction has completed as well:
        updatingTransaction.get();

        // Now we can release the lock blocking the populator, allowing it to finish:
        lockBlockingIndexPopulator.release();

        // And return the future for examination:
        return createConstraintTransaction;
    }

    // -- Set Up: Data parts

    // These two values coalesce to the same double value, and therefor collides in our current index implementation:
    private static final long COLLISION_X = 4611686018427387905L;
    private static final long COLLISION_Y = 4611686018427387907L;
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    private Label label = DynamicLabel.label( "Cybermen" );
    private String property = "name";
    private Node a;
    private Node b;
    private Node c;
    private Node d;

    private GraphDatabaseService db;

    /**
     * Effectively:
     *
     * <pre><code>
     *     CREATE CONSTRAINT ON (n:Cybermen) assert n.name is unique
     *     ;
     *
     *     CREATE (a:Cybermen {name: "a"}),
     *            (b:Cybermen),
     *            (c: {name: "a"}),
     *            (d: {name: "d"})
     *     ;
     * </code></pre>
     */
    private void givenOnlineConstraint()
    {
        createUniqueConstraint();
        try ( Transaction tx = db.beginTx() )
        {
            a = db.createNode( label );
            a.setProperty( property, "a" );
            b = db.createNode( label );
            c = db.createNode();
            c.setProperty( property, "a" );
            d = db.createNode();
            d.setProperty( property, "d" );
            tx.success();
        }
    }

    /**
     * Effectively:
     *
     * <pre><code>
     *     CREATE (a:Cybermen {name: "a"}),
     *            (b:Cybermen {name: "b"}),
     *            (c:Cybermen {name: "c"}),
     *            (d:Cybermen {name: "d"})
     *     ;
     * </code></pre>
     */
    private void givenUniqueDataset()
    {
        try ( Transaction tx = db.beginTx() )
        {
            a = db.createNode( label );
            a.setProperty( property, "a" );
            b = db.createNode( label );
            b.setProperty( property, "b" );
            c = db.createNode( label );
            c.setProperty( property, "c" );
            d = db.createNode( label );
            d.setProperty( property, "d" );
            tx.success();
        }
    }

    /**
     * Effectively:
     *
     * <pre><code>
     *     CREATE CONSTRAINT ON (n:Cybermen) assert n.name is unique
     *     ;
     * </code></pre>
     */
    private void createUniqueConstraint()
    {
        createUniqueConstraint( null );
    }

    /**
     * Effectively:
     *
     * <pre><code>
     *     CREATE CONSTRAINT ON (n:Cybermen) assert n.name is unique
     *     ;
     * </code></pre>
     *
     * Also counts down the given latch prior to creating the constraint.
     */
    private void createUniqueConstraint( CountDownLatch preCreateLatch )
    {
        try ( Transaction tx = db.beginTx() )
        {
            if ( preCreateLatch != null )
            {
                preCreateLatch.countDown();
            }
            db.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
            tx.success();
        }
    }

    /**
     * Effectively:
     *
     * <pre><code>
     *     return single( db.findNodesByLabelAndProperty( label, property, value ), null );
     * </code></pre>
     */
    private Node lookUpNode( Object value )
    {
        return db.findNode( label, property, value );
    }

    // -- Set Up: Transaction handling

    public void transaction( Action... actions )
    {
        int progress = 0;
        try ( Transaction tx = db.beginTx() )
        {
            for ( Action action : actions )
            {
                action.accept( tx );
                progress++;
            }
        }
        catch ( Throwable ex )
        {
            StringBuilder sb = new StringBuilder( "Transaction failed:\n\n" );
            for ( int i = 0; i < actions.length; i++ )
            {
                String mark = progress == i? " failed --> " : "            ";
                sb.append( mark ).append( actions[i] ).append( '\n' );
            }
            ex.addSuppressed( new AssertionError( sb.toString() ) );
            throw ex;
        }
    }

    private abstract class Action implements Consumer<Transaction>
    {
        private final String name;

        protected Action( String name )
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    private final Action success = new Action( "tx.success();" )
    {
        @Override
        public void accept( Transaction transaction )
        {
            transaction.success();
            // We also call close() here, because some validations and checks don't run until commit
            transaction.close();
        }
    };

    private Action createNode( final Object propertyValue )
    {
        return new Action( "Node node = db.createNode( label ); " +
                "node.setProperty( property, " + reprValue( propertyValue ) + " );" )
        {
            @Override
            public void accept( Transaction transaction )
            {
                Node node = db.createNode( label );
                node.setProperty( property, propertyValue );
            }
        };
    }

    private Action setProperty( final Node node, final Object value )
    {
        return new Action( reprNode( node ) + ".setProperty( property, " + reprValue( value ) + " );")
        {
            @Override
            public void accept( Transaction transaction )
            {
                node.setProperty( property, value );
            }
        };
    }

    private Action removeProperty( final Node node )
    {
        return new Action( reprNode( node ) + ".removeProperty( property );")
        {
            @Override
            public void accept( Transaction transaction )
            {
                node.removeProperty( property );
            }
        };
    }

    private Action addLabel( final Node node, final Label label )
    {
        return new Action( reprNode( node ) + ".addLabel( " + label + " );" )
        {
            @Override
            public void accept( Transaction transaction )
            {
                node.addLabel( label );
            }
        };
    }

    private Action fail( final String message )
    {
        return new Action( "fail( \"" + message + "\" );")
        {
            @Override
            public void accept( Transaction transaction )
            {
                Assert.fail( message );
            }
        };
    }

    private Action assertLookupNode( final Object propertyValue, final Matcher<Node> matcher )
    {
        return new Action( "assertThat( lookUpNode( " + reprValue( propertyValue ) + " ), " + matcher + " );" )
        {
            @Override
            public void accept( Transaction transaction )
            {
                assertThat( lookUpNode( propertyValue ), matcher );
            }
        };
    }

    private String reprValue( Object value )
    {
        return value instanceof String? "\"" + value + "\"" : String.valueOf( value );
    }

    private String reprNode( Node node )
    {
        return node == a? "a"
                : node == b? "b"
                : node == c? "c"
                : node == d? "d"
                : "n";
    }

    // -- Set Up: Advanced transaction handling

    private final Map<Transaction, TopLevelTransaction> txMap = new IdentityHashMap<>();

    private void suspend( Transaction tx ) throws Exception
    {
        ThreadToStatementContextBridge txManager = getTransactionManager();
        txMap.put( tx, txManager.getTopLevelTransactionBoundToThisThread( true ) );
        txManager.unbindTransactionFromCurrentThread();
    }

    private void resume( Transaction tx ) throws Exception
    {
        ThreadToStatementContextBridge txManager = getTransactionManager();
        txManager.bindTransactionToCurrentThread( txMap.remove(tx) );
    }

    private ThreadToStatementContextBridge getTransactionManager()
    {
        return resolveInternalDependency( ThreadToStatementContextBridge.class );
    }

    // -- Set Up: Misc. sharp tools

    /**
     * Locks controlling concurrent access to the store files.
     */
    private LockService getLockService()
    {
        return resolveInternalDependency( LockService.class );
    }

    private <T> T resolveInternalDependency( Class<T> type )
    {
        @SuppressWarnings("deprecation")
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        DependencyResolver resolver = api.getDependencyResolver();
        return resolver.resolveDependency( type );
    }

    private static void awaitUninterruptibly( CountDownLatch latch )
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            throw new AssertionError( "Interrupted", e );
        }
    }

    // -- Set Up: Environment parts

    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Before
    public void setUp() {
        File storeDir = testDirectory.graphDbDir();
        TestGraphDatabaseFactory dbfactory = new TestGraphDatabaseFactory();
        dbfactory.addKernelExtension( new PredefinedSchemaIndexProviderFactory( indexProvider ) );
        db = dbfactory.newImpermanentDatabase( storeDir );
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    private static class PredefinedSchemaIndexProviderFactory extends KernelExtensionFactory<PredefinedSchemaIndexProviderFactory.NoDeps>
    {
        private final SchemaIndexProvider indexProvider;

        @Override
        public Lifecycle newKernelExtension( NoDeps noDeps ) throws Throwable
        {
            return indexProvider;
        }

        public static interface NoDeps {
        }

        public PredefinedSchemaIndexProviderFactory( SchemaIndexProvider indexProvider )
        {
            super( indexProvider.getClass().getSimpleName() );
            this.indexProvider = indexProvider;
        }
    }
}
