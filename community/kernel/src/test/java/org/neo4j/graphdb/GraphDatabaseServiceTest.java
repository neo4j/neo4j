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
package org.neo4j.graphdb;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class GraphDatabaseServiceTest
{
    @Test
    public void givenShutdownDatabaseWhenBeginTxThenExceptionIsThrown() throws Exception
    {
        // Given
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        db.shutdown();

        // When
        try
        {
            Transaction tx = db.beginTx();
            Assert.fail();
        }
        catch ( Exception e )
        {
            // Then
            Assert.assertThat( e.getClass().getName(), CoreMatchers.equalTo( TransactionFailureException.class
                    .getName() ) );
        }
    }

    @Test
    public void givenDatabaseAndStartedTxWhenShutdownThenWaitForTxToFinish() throws Exception
    {
        // Given
        final GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // When
        final CountDownLatch started = new CountDownLatch( 1 );
        Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                final Transaction tx = db.beginTx();

                started.countDown();

                try
                {
                    Thread.sleep( 2000 );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }

                db.createNode().setProperty( "foo", "bar" );
                tx.success();
                tx.finish();
            }
        } );

        started.await();
        db.shutdown();


    }

    @Test
    public void givenDatabaseAndStartedTxWhenShutdownAndStartNewTxThenBeginTxTimesOut() throws Exception
    {
        // Given
        final GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // When
        final CountDownLatch started = new CountDownLatch( 1 );
        final AtomicReference result = new AtomicReference();
        Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                final Transaction tx = db.beginTx();

                started.countDown();

                try
                {
                    Thread.sleep( 2000 );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }

                db.createNode().setProperty( "foo", "bar" );
                tx.success();

                Executors.newSingleThreadExecutor().submit( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            db.beginTx();
                            result.set( Boolean.TRUE );
                        }
                        catch ( Exception e )
                        {
                            result.set( e );
                        }

                        synchronized ( result )
                        {
                            result.notifyAll();
                        }
                    }
                } );

                tx.finish();
            }
        } );

        started.await();
        db.shutdown();

        while ( result.get() == null )
        {
            synchronized ( result )
            {
                result.wait( 100 );
            }
        }

        Assert.assertThat( result.get().getClass(), CoreMatchers.<Object>equalTo( TransactionFailureException.class ) );
    }
}
