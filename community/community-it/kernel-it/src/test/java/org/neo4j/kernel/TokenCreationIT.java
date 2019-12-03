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
package org.neo4j.kernel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterables.asSet;

/**
 * Token creation should be able to handle cases of concurrent token creation
 * with different/same names. Short random interval (1-3) give a high chances of same token name in this test.
 * <p>
 * Newly created token should be visible only when token cache already have both mappings:
 * "name -> id" and "id -> name" populated.
 * Otherwise attempt to retrieve labels from newly created node can fail.
 */
@DbmsExtension
class TokenCreationIT
{
    @Inject
    private GraphDatabaseService db;

    private volatile boolean stop;
    private ExecutorService executorService;

    @BeforeEach
    void setUp()
    {
        executorService = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown()
    {
        executorService.shutdown();
    }

    @RepeatedTest( 5 )
    void concurrentLabelTokenCreation() throws InterruptedException, ExecutionException
    {
        int concurrentWorkers = 10;
        CountDownLatch latch = new CountDownLatch( concurrentWorkers );
        List<Future<?>> futures = new ArrayList<>();
        for ( int i = 0; i < concurrentWorkers; i++ )
        {
            futures.add( executorService.submit( new LabelCreator( db, latch ) ) );
        }
        LockSupport.parkNanos( MILLISECONDS.toNanos( 500 ) );
        stop = true;
        latch.await();
        consumeFutures( futures );
    }

    private void consumeFutures( List<Future<?>> futures ) throws InterruptedException, ExecutionException
    {
        for ( Future<?> future : futures )
        {
            future.get();
        }
    }

    private Label[] getLabels()
    {
        int randomLabelValue = ThreadLocalRandom.current().nextInt( 2 ) + 1;
        Label[] labels = new Label[randomLabelValue];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = Label.label( randomAlphanumeric( randomLabelValue ) );
        }
        return labels;
    }

    private class LabelCreator implements Runnable
    {
        private final GraphDatabaseService database;
        private final CountDownLatch createLatch;

        LabelCreator( GraphDatabaseService database, CountDownLatch createLatch )
        {
            this.database = database;
            this.createLatch = createLatch;
        }

        @Override
        public void run()
        {
            try
            {
                while ( !stop )
                {

                    try ( Transaction transaction = database.beginTx() )
                    {
                        Label[] createdLabels = getLabels();
                        Node node = transaction.createNode( createdLabels );
                        Iterable<Label> nodeLabels = node.getLabels();
                        assertEquals( asSet( asList( createdLabels ) ), asSet( nodeLabels ) );
                        transaction.commit();
                    }
                    catch ( Exception e )
                    {
                        stop = true;
                        throw e;
                    }
                }
            }
            finally
            {
                createLatch.countDown();
            }
        }
    }
}
