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
package org.neo4j.kernel.impl.transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;

@ExtendWith( TestDirectoryExtension.class )
class CommitContentionTest
{
    @Inject
    private TestDirectory testDirectory;

    private final Semaphore semaphore1 = new Semaphore( 1 );
    private final Semaphore semaphore2 = new Semaphore( 1 );
    private final AtomicReference<Exception> reference = new AtomicReference<>();

    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void before() throws Exception
    {
        semaphore1.acquire();
        semaphore2.acquire();
        db = createDb();
    }

    @AfterEach
    void after()
    {
        managementService.shutdown();
    }

    @Test
    void shouldNotContendOnCommitWhenPushingUpdates() throws Exception
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
        managementService = new DatabaseManagementServiceFactory( DatabaseInfo.COMMUNITY, globalModule -> new CommunityEditionModule( globalModule )
        {
            @Override
            public DatabaseTransactionStats createTransactionMonitor()
            {
                return new SkipTransactionDatabaseStats();
            }
        } ).newFacade( testDirectory.storeDir(), Config.defaults(), GraphDatabaseDependencies.newDependencies() );
        return managementService
                .database( Config.defaults().get( GraphDatabaseSettings.default_database ));
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

    private class SkipTransactionDatabaseStats extends DatabaseTransactionStats
    {
        boolean skip;

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
    }
}
