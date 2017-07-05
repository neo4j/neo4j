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
package org.neo4j.kernel.impl.api;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.logging.NullLog;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.Iterables.single;

/**
 * In its essence this test is about a race in mainly {@link DatabaseAvailability} and
 * {@link TransactionMonitor#transactionStarted()} where a transaction thread (starting the transaction)
 * would race with shutdown ({@link DatabaseAvailability#stop()}) where a transaction would get passed
 * the availability check, the shutdown would think that no transactions were running and start the shutdown,
 * leaving the transaction executing straight in the middle of dead or dying components.
 *
 * This will still be the case for transactions that live longer than the shutdown timeout in
 * {@link DatabaseAvailability#stop()}, but at least that's deterministic and configurable.
 */
public class TransactionCompletionAndShutdownRaceIT
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();
    @Rule
    public final OtherThreadRule<Void> transactor = new OtherThreadRule<>( "Transactor" );
    @Rule
    public final OtherThreadRule<Void> shutter = new OtherThreadRule<>( "Shutter" );
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldAlwaysAwaitTransactionCompletionBeforeShuttingDown() throws Exception
    {
        // GIVEN
        Barrier.Control barrier = new Barrier.Control();
        AtomicBoolean barrierInstalled = new AtomicBoolean();
        File storeDir = directory.absolutePath();
        TestGraphDatabaseFactory dbFactory =
                dbFactoryWithBarrierControlledTransactionStats( barrier, barrierInstalled );

        {
            GraphDatabaseService db = dbFactory.newImpermanentDatabase( storeDir );
            barrierInstalled.set( true );

            // WHEN
            Future<Object> transactionFuture = transactor.execute( state -> doTransaction( db, barrier ) );
            Future<Object> shutterFuture = shutter.execute( state -> doShutdown( db, barrier ) );

            // THEN
            shutterFuture.get();
            transactionFuture.get();
            barrierInstalled.set( false );
        }

        // Now assert that the node was created
        GraphDatabaseService db = dbFactory.newImpermanentDatabase( storeDir );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = single( db.getAllNodes() );
            assertNotNull( node );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private TestGraphDatabaseFactory dbFactoryWithBarrierControlledTransactionStats( Barrier.Control barrier,
            AtomicBoolean barrierInstaller )
    {
        return new TestGraphDatabaseFactory()
        {
            @Override
            protected GraphDatabaseFacadeFactory newTestGraphDatabaseFacadeFactory(
                    File storeDir, Config config, TestGraphDatabaseFactoryState state )
            {
                return new TestGraphDatabaseFacadeFactory( state, true )
                {
                    @Override
                    protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                            GraphDatabaseFacade graphDatabaseFacade )
                    {
                        return new TestGraphDatabaseFacadeFactory.TestDatabasePlatformModule(
                                storeDir, config, databaseInfo, dependencies, graphDatabaseFacade )
                        {
                            @Override
                            protected AvailabilityGuard createAvailabilityGuard()
                            {
                                return new AvailabilityGuard( clock, NullLog.getInstance() )
                                {
                                    @Override
                                    public void require( AvailabilityRequirement requirement )
                                    {
                                        super.require( requirement );
                                        if ( barrierInstaller.get() )
                                        {
                                            barrier.release();
                                        }
                                    }

                                    @Override
                                    public void await( long millis ) throws UnavailableException
                                    {
                                        super.await( millis );
                                    }
                                };
                            }
                        };
                    }
                };
            }
        };
    }

    private Object doShutdown( GraphDatabaseService db, Barrier.Control barrier )
    {
        barrier.awaitUninterruptibly();
        db.shutdown();
        return null;
    }

    private Object doTransaction( GraphDatabaseService db, Barrier.Control barrier )
    {
        try ( Transaction tx = db.beginTx() )
        {
            barrier.reached();
            db.createNode();
            tx.success();
        }
        return null;
    }

    @Test
    public void shouldNotStartTransactoinOnDatabaseThatIsKnownToBeShuttingDown() throws Exception
    {
        // GIVEN
        Barrier.Control barrier = new Barrier.Control();
        AtomicBoolean barrierInstalled = new AtomicBoolean();
        File storeDir = directory.absolutePath();
        TestGraphDatabaseFactory dbFactory = dbFactoryWithBarrierControlledTransactionStats2( barrier,
                barrierInstalled );

        {
            GraphDatabaseService db = dbFactory.newImpermanentDatabase( storeDir );
            barrierInstalled.set( true );

            // WHEN
            Future<Object> transactionFuture = transactor.execute( state -> doTransaction( db ) );
            Future<Object> shutterFuture = shutter.execute( state -> doShutdown( db, barrier ) );

            // THEN
            shutterFuture.get();
            expectedException.expectCause( isA( DatabaseShutdownException.class ) );
            transactionFuture.get();
            barrierInstalled.set( false );
        }
    }

    private TestGraphDatabaseFactory dbFactoryWithBarrierControlledTransactionStats2( Barrier.Control barrier,
            AtomicBoolean barrierInstaller )
    {
        return new TestGraphDatabaseFactory()
        {
            @Override
            protected GraphDatabaseFacadeFactory newTestGraphDatabaseFacadeFactory( File storeDir, Config config,
                    TestGraphDatabaseFactoryState state )
            {
                return new TestGraphDatabaseFacadeFactory( state, true )
                {
                    @Override
                    protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                            GraphDatabaseFacade graphDatabaseFacade )
                    {
                        return new TestGraphDatabaseFacadeFactory.TestDatabasePlatformModule( storeDir, config,
                                databaseInfo, dependencies, graphDatabaseFacade )
                        {
                            @Override
                            protected AvailabilityGuard createAvailabilityGuard()
                            {
                                return new AvailabilityGuard( clock, NullLog.getInstance() )
                                {
                                    @Override
                                    public void require( AvailabilityRequirement requirement )
                                    {
                                        super.require( requirement );
                                        if ( barrierInstaller.get() )
                                        {
                                            barrier.release();
                                        }
                                    }

                                    @Override
                                    public void await( long millis ) throws UnavailableException
                                    {
                                        super.await( millis );
                                        if ( barrierInstaller.get() )
                                        {
                                            barrier.reached();
                                        }
                                    }
                                };
                            }
                        };
                    }
                };
            }
        };
    }

    private Object doTransaction( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        return null;
    }
}
