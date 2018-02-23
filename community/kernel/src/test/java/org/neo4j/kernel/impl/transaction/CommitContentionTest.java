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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;

@ExtendWith( TestDirectoryExtension.class )
public class CommitContentionTest
{
    @Resource
    public TestDirectory storeLocation;

    final Semaphore semaphore1 = new Semaphore( 1 );
    final Semaphore semaphore2 = new Semaphore( 1 );
    final AtomicReference<Exception> reference = new AtomicReference<>();

    private GraphDatabaseService db;

    @BeforeEach
    public void before() throws Exception
    {
        semaphore1.acquire();
        semaphore2.acquire();
        db = createDb();
    }

    @AfterEach
    public void after()
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
        try ( Transaction transaction = db.beginTx() )
        {
            db.createNode();
            transaction.success();
        }
    }

    private Thread startFirstTransactionWhichBlocksDuringPushUntilSecondTransactionFinishes() throws
            InterruptedException
    {
        Thread thread = new Thread( this::createNode );

        thread.start();

        waitForFirstTransactionToStartPushing();

        return thread;
    }

    private GraphDatabaseService createDb()
    {
        GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
        //noinspection deprecation
        return new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new )
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                    GraphDatabaseFacade graphDatabaseFacade )
            {
                return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade )
                {
                    @Override
                    protected TransactionStats createTransactionStats()
                    {
                        return new TransactionStats()
                        {
                            public boolean skip;

                            @Override
                            public void transactionFinished( boolean committed, boolean write )
                            {
                                super.transactionFinished( committed, write );

                                if ( committed )
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
                        };
                    }
                };
            }
        }.newFacade( storeLocation.graphDbDir(), Config.defaults(), state.databaseDependencies() );
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
        }
        catch ( InterruptedException e )
        {
            reference.set( e );
        }
    }
}
