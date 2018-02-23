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
package org.neo4j.locking;

import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import javax.annotation.Resource;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.community.CommunityLockClient;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.rules.ExpectedException.none;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseBuilder.DatabaseCreator;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.lock_acquisition_timeout;
import static org.neo4j.kernel.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.COMMUNITY;
import static org.neo4j.kernel.impl.locking.LockTracer.NONE;
import static org.neo4j.kernel.impl.locking.ResourceTypes.LABEL;
import static org.neo4j.test.OtherThreadExecutor.WaitDetails;

@EnableRuleMigrationSupport
@ExtendWith( TestDirectoryExtension.class )
public class CommunityLockAcquisitionTimeoutIT
{

    @Resource
    private TestDirectory directory;
    @Rule
    public final ExpectedException expectedException = none();

    private final OtherThreadExecutor<Void> secondTransactionExecutor =
            new OtherThreadExecutor<>( "transactionExecutor", null );
    private final OtherThreadExecutor<Void> clockExecutor = new OtherThreadExecutor<>( "clockExecutor", null );

    private static final int TEST_TIMEOUT = 5000;
    private static final String TEST_PROPERTY_NAME = "a";
    private static final Label marker = label( "marker" );
    private static final FakeClock fakeClock = Clocks.fakeClock();

    private static GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        CustomClockFacadeFactory facadeFactory = new CustomClockFacadeFactory();
        database = new CustomClockTestGraphDatabaseFactory( facadeFactory )
                .newEmbeddedDatabaseBuilder( directory.graphDbDir() )
                .setConfig( lock_acquisition_timeout, "2s" )
                .setConfig( "dbms.backup.enabled", "false" ).newGraphDatabase();

        createTestNode( marker );
    }

    @AfterEach
    void tearDown()
    {
        secondTransactionExecutor.close();
        clockExecutor.close();
        database.shutdown();
    }

    @Test
    void timeoutOnAcquiringExclusiveLock() throws Exception
    {
        assertTimeout( ofMillis( TEST_TIMEOUT ), () -> {
            expectedException.expect( new RootCauseMatcher<>( LockAcquisitionTimeoutException.class,
                    "The transaction has been terminated. " +
                            "Retry your operation in a new transaction, and you should see a successful result. " +
                            "Unable to acquire lock within configured timeout. " +
                            "Unable to acquire lock for resource: NODE with id: 0 within 2000 millis." ) );

            try ( Transaction ignored = database.beginTx() )
            {
                ResourceIterator<Node> nodes = database.findNodes( marker );
                Node node = nodes.next();
                node.setProperty( TEST_PROPERTY_NAME, "b" );

                Future<Void> propertySetFuture = secondTransactionExecutor.executeDontWait( state -> {
                    try ( Transaction transaction1 = database.beginTx() )
                    {
                        node.setProperty( TEST_PROPERTY_NAME, "b" );
                        transaction1.success();
                    }
                    return null;
                } );

                secondTransactionExecutor.waitUntilWaiting( exclusiveLockWaitingPredicate() );
                clockExecutor.execute( (OtherThreadExecutor.WorkerCommand<Void,Void>) state -> {
                    fakeClock.forward( 3, SECONDS );
                    return null;
                } );
                propertySetFuture.get();

                fail( "Should throw termination exception." );
            }
        } );
    }

    @Test
    void timeoutOnAcquiringSharedLock() throws Exception
    {
        assertTimeout( ofMillis( TEST_TIMEOUT ), () -> {
            expectedException.expect( new RootCauseMatcher<>( LockAcquisitionTimeoutException.class,
                    "The transaction has been terminated. " +
                            "Retry your operation in a new transaction, and you should see a successful result. " +
                            "Unable to acquire lock within configured timeout. " +
                            "Unable to acquire lock for resource: LABEL with id: 1 within 2000 millis." ) );

            try ( Transaction ignored = database.beginTx() )
            {
                Locks lockManger = getLockManager();
                lockManger.newClient().acquireExclusive( NONE, LABEL, 1 );

                Future<Void> propertySetFuture = secondTransactionExecutor.executeDontWait( state -> {
                    try ( Transaction nestedTransaction = database.beginTx() )
                    {
                        ResourceIterator<Node> nodes = database.findNodes( marker );
                        Node node = nodes.next();
                        node.addLabel( label( "anotherLabel" ) );
                        nestedTransaction.success();
                    }
                    return null;
                } );

                secondTransactionExecutor.waitUntilWaiting( sharedLockWaitingPredicate() );
                clockExecutor.execute( (OtherThreadExecutor.WorkerCommand<Void,Void>) state -> {
                    fakeClock.forward( 3, SECONDS );
                    return null;
                } );
                propertySetFuture.get();

                fail( "Should throw termination exception." );
            }
        } );
    }

    protected Locks getLockManager()
    {
        return getDependencyResolver().resolveDependency( CommunityLockManger.class );
    }

    protected DependencyResolver getDependencyResolver()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver();
    }

    protected Predicate<WaitDetails> exclusiveLockWaitingPredicate()
    {
        return waitDetails -> waitDetails.isAt( CommunityLockClient.class, "acquireExclusive" );
    }

    protected Predicate<WaitDetails> sharedLockWaitingPredicate()
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

    private static class CustomClockTestGraphDatabaseFactory extends TestGraphDatabaseFactory
    {
        private GraphDatabaseFacadeFactory customFacadeFactory;

        CustomClockTestGraphDatabaseFactory( GraphDatabaseFacadeFactory customFacadeFactory )
        {
            this.customFacadeFactory = customFacadeFactory;
        }

        @Override
        protected DatabaseCreator createDatabaseCreator( File storeDir,
                GraphDatabaseFactoryState state )
        {
            return new DatabaseCreator()
            {
                @Override
                public GraphDatabaseService newDatabase( Map<String,String> config )
                {
                    return newDatabase( defaults( config ) );
                }

                @Override
                public GraphDatabaseService newDatabase( Config config )
                {
                    return customFacadeFactory.newFacade( storeDir, config,
                            newDependencies( state.databaseDependencies() ) );
                }
            };
        }
    }

    private static class CustomClockFacadeFactory extends GraphDatabaseFacadeFactory
    {

        CustomClockFacadeFactory()
        {
            super( COMMUNITY, CommunityEditionModule::new );
        }

        @Override
        protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                GraphDatabaseFacade graphDatabaseFacade )
        {
            return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade )
            {
                @Override
                protected SystemNanoClock createClock()
                {
                    return fakeClock;
                }
            };
        }

    }
}
