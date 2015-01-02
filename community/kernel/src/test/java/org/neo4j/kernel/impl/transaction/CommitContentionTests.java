/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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


import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import javax.transaction.xa.XAException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.NoCacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.test.TargetDirectory;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.InternalAbstractGraphDatabase.Configuration.cache_type;
import static org.neo4j.test.TargetDirectory.forTest;

public class CommitContentionTests
{
    private static final TargetDirectory target = forTest( CommitContentionTests.class );

    final Semaphore semaphore1 = new Semaphore( 1 );
    final Semaphore semaphore2 = new Semaphore( 1 );
    final AtomicReference<Exception> reference = new AtomicReference<>();

    final TxIdGenerator txIdGenerator = new TxIdGenerator()
    {
        public boolean skip;

        @Override
        public long generate( XaDataSource dataSource, int identifier ) throws XAException
        {
            return dataSource.getLastCommittedTxId() + 1;
        }

        @Override
        public void committed( XaDataSource dataSource, int identifier, long txId,
                               Integer externalAuthorServerId )
        {
            // skip signal and waiting for second transaction
            if ( skip == true )
            {
                return;
            }
            skip = true;

            signalFirstTransactionStartedPushing();

            waitForSecondTransactionToFinish();
        }

        @Override
        public int getCurrentMasterId()
        {
            return 42;
        }

        @Override
        public int getMyId()
        {
            return 87;
        }
    };

    @Rule
    public TargetDirectory.TestDirectory storeLocation = target.testDirectory();

    private GraphDatabaseService db;

    @Before
    public void before() throws Exception
    {
        semaphore1.acquire();
        semaphore2.acquire();
        db = createDb( txIdGenerator );
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
        try ( Transaction transaction = db.beginTx() )
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

    private GraphDatabaseService createDb( final TxIdGenerator idGenerator )
    {
        GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
        state.setCacheProviders( asList( (CacheProvider) new NoCacheProvider() ) );
        state.setTransactionInterceptorProviders( Arrays.<TransactionInterceptorProvider>asList() );
        //noinspection deprecation
        return new EmbeddedGraphDatabase( storeLocation.absolutePath(), stringMap( cache_type.name(),
                NoCacheProvider.NAME ), state.databaseDependencies() )
        {
            @Override
            protected TxIdGenerator createTxIdGenerator()
            {
                return idGenerator;
            }
        };
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
