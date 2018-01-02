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
package org.neo4j.kernel.impl.transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CommitContentionTests
{
    @Rule
    public final TargetDirectory.TestDirectory storeLocation =
            TargetDirectory.testDirForTest( CommitContentionTests.class );

    final Semaphore semaphore1 = new Semaphore( 1 );
    final Semaphore semaphore2 = new Semaphore( 1 );
    final AtomicReference<Exception> reference = new AtomicReference<>();

    private GraphDatabaseService db;

    @Before
    public void before() throws Exception
    {
        semaphore1.acquire();
        semaphore2.acquire();
        db = createDb();
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    @Test
    public void shouldNotContendOnCommitWhenPushingUpdates() throws Exception
    {
        Thread thread = startFirstTransactionWhichBlocksDuringPushUntilSecondTransactionFinishes();

        runAndFinishSecondTransaction();

        thread.join();

        assertNoFailures();
    }

    private void assertNoFailures()
    {
        Exception e = reference.get();

        if ( e != null )
        {
            throw new AssertionError( e );
        }
    }

    private void runAndFinishSecondTransaction()
    {
        createNode();

        signalSecondTransactionFinished();
    }

    private void createNode()
    {
        try (Transaction transaction = db.beginTx())
        {
            db.createNode();
            transaction.success();
        }
    }

    private Thread startFirstTransactionWhichBlocksDuringPushUntilSecondTransactionFinishes() throws
            InterruptedException
    {
        Thread thread = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                createNode();
            }
        } );

        thread.start();

        waitForFirstTransactionToStartPushing();

        return thread;
    }

    private GraphDatabaseService createDb()
    {
        GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
        //noinspection deprecation
        return new CommunityFacadeFactory()
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Map<String, String> params,
                    Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                    OperationalMode operationalMode )
            {
                return new PlatformModule( storeDir, params, dependencies, graphDatabaseFacade, operationalMode )
                {
                    @Override
                    protected TransactionCounters createTransactionCounters()
                    {
                        return new TransactionCounters()
                        {
                            public boolean skip;

                            @Override
                            public void transactionFinished( boolean successful )
                            {
                                super.transactionFinished( successful );

                                if ( isTheRemoveOrphanedConstraintIndexesOnStartupTransaction() )
                                {
                                    return;
                                }


                                if ( successful )
                                {
                                    // skip signal and waiting for second transaction
                                    if ( skip )
                                    {
                                        return;
                                    }
                                    skip = true;

                                    signalFirstTransactionStartedPushing();

                                    waitForSecondTransactionToFinish();
                                }
                            }

                            private boolean isTheRemoveOrphanedConstraintIndexesOnStartupTransaction()
                            {
                                for ( StackTraceElement element : Thread.currentThread().getStackTrace() )
                                {
                                    if ( element.getClassName().contains( RemoveOrphanConstraintIndexesOnStartup.class.getSimpleName() ) )
                                    {
                                        return true;
                                    }
                                }
                                return false;
                            }
                        };
                    }
                };
            }
        }.newFacade( storeLocation.graphDbDir(), stringMap(), state.databaseDependencies() );
    }

    private void waitForFirstTransactionToStartPushing() throws InterruptedException
    {
        if ( !semaphore1.tryAcquire( 10, SECONDS ) )
        {
            throw new IllegalStateException( "First transaction never started pushing" );
        }
    }

    private void signalFirstTransactionStartedPushing()
    {
        semaphore1.release();
    }

    private void signalSecondTransactionFinished()
    {
        semaphore2.release();
    }

    private void waitForSecondTransactionToFinish()
    {
        try
        {
            boolean acquired = semaphore2.tryAcquire( 10, SECONDS );

            if ( !acquired )
            {
                reference.set( new IllegalStateException( "Second transaction never finished" ) );
            }
        } catch ( InterruptedException e )
        {
            reference.set( e );
        }
    }
}
