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
package org.neo4j.locking;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockClient;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class CommunityLockAcquisitionTimeoutIT
{
    @ClassRule
    public static final TestDirectory directory = TestDirectory.testDirectory();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final OtherThreadExecutor<Void> secondTransactionExecutor =
            new OtherThreadExecutor<>( "transactionExecutor", null );
    private final OtherThreadExecutor<Void> clockExecutor = new OtherThreadExecutor<>( "clockExecutor", null );

    private static final int TEST_TIMEOUT = 5000;
    private static final String TEST_PROPERTY_NAME = "a";
    private static final Label marker = Label.label( "marker" );
    private static final FakeClock fakeClock = Clocks.fakeClock();

    private static GraphDatabaseService database;
    private static DatabaseManagementService managementService;

    @BeforeClass
    public static void setUp()
    {
        managementService = new TestDatabaseManagementServiceBuilder( directory.storeDir() )
                .setClock( fakeClock )
                .setConfig( GraphDatabaseSettings.lock_acquisition_timeout, "2s" )
                .setConfig( "dbms.backup.enabled", "false" )
                .build();
        database = managementService.database( DEFAULT_DATABASE_NAME );

        createTestNode( marker );
    }

    @AfterClass
    public static void tearDownClass()
    {
        managementService.shutdown();
    }

    @After
    public void tearDown()
    {
        secondTransactionExecutor.close();
        clockExecutor.close();
    }

    @Test( timeout = TEST_TIMEOUT )
    public void timeoutOnAcquiringExclusiveLock() throws Exception
    {
        expectedException.expect( new RootCauseMatcher<>( LockAcquisitionTimeoutException.class,
                "The transaction has been terminated. " +
                        "Retry your operation in a new transaction, and you should see a successful result. " +
                        "Unable to acquire lock within configured timeout (dbms.lock.acquisition.timeout). " +
                        "Unable to acquire lock for resource: NODE with id: 0 within 2000 millis." ) );

        try ( Transaction ignored = database.beginTx() )
        {
            ResourceIterator<Node> nodes = database.findNodes( marker );
            Node node = nodes.next();
            node.setProperty( TEST_PROPERTY_NAME, "b" );

            Future<Void> propertySetFuture = secondTransactionExecutor.executeDontWait( state ->
            {
                try ( Transaction transaction1 = database.beginTx() )
                {
                    node.setProperty( TEST_PROPERTY_NAME, "b" );
                    transaction1.success();
                }
                return null;
            } );

            secondTransactionExecutor.waitUntilWaiting( exclusiveLockWaitingPredicate() );
            clockExecutor.execute( (OtherThreadExecutor.WorkerCommand<Void,Void>) state ->
            {
                fakeClock.forward( 3, TimeUnit.SECONDS );
                return null;
            } );
            propertySetFuture.get();

            fail( "Should throw termination exception." );
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void timeoutOnAcquiringSharedLock() throws Exception
    {
        expectedException.expect( new RootCauseMatcher<>( LockAcquisitionTimeoutException.class,
                "The transaction has been terminated. " +
                        "Retry your operation in a new transaction, and you should see a successful result. " +
                        "Unable to acquire lock within configured timeout (dbms.lock.acquisition.timeout). " +
                        "Unable to acquire lock for resource: LABEL with id: 1 within 2000 millis." ) );

        try ( Transaction ignored = database.beginTx() )
        {
            Locks lockManger = getLockManager();
            lockManger.newClient().acquireExclusive( LockTracer.NONE, ResourceTypes.LABEL, 1 );

            Future<Void> propertySetFuture = secondTransactionExecutor.executeDontWait( state ->
            {
                try ( Transaction nestedTransaction = database.beginTx() )
                {
                    ResourceIterator<Node> nodes = database.findNodes( marker );
                    Node node = nodes.next();
                    node.addLabel( Label.label( "anotherLabel" ) );
                    nestedTransaction.success();
                }
                return null;
            } );

            secondTransactionExecutor.waitUntilWaiting( sharedLockWaitingPredicate() );
            clockExecutor.execute( (OtherThreadExecutor.WorkerCommand<Void,Void>) state ->
            {
                fakeClock.forward( 3, TimeUnit.SECONDS );
                return null;
            } );
            propertySetFuture.get();

            fail( "Should throw termination exception." );
        }
    }

    protected Locks getLockManager()
    {
        return getDependencyResolver().resolveDependency( CommunityLockManger.class );
    }

    protected DependencyResolver getDependencyResolver()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver();
    }

    protected Predicate<OtherThreadExecutor.WaitDetails> exclusiveLockWaitingPredicate()
    {
        return waitDetails -> waitDetails.isAt( CommunityLockClient.class, "acquireExclusive" );
    }

    protected Predicate<OtherThreadExecutor.WaitDetails> sharedLockWaitingPredicate()
    {
        return waitDetails -> waitDetails.isAt( CommunityLockClient.class, "acquireShared" );
    }

    private static void createTestNode( Label marker )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode( marker );
            transaction.success();
        }
    }
}
