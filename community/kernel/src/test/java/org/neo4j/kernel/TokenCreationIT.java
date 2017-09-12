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
package org.neo4j.kernel;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RepeatRule;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.asSet;

/**
 * Token creation should be able to handle cases of concurrent token creation
 * with different/same names. Short random interval (1-3) give a high chances of same token name in this test.
 * <p>
 * Newly created token should be visible only when token cache already have both mappings:
 * "name -> id" and "id -> name" populated.
 * Otherwise attempt to retrieve labels from newly created node can fail.
 */
public class TokenCreationIT
{
    @Rule
    public final EmbeddedDatabaseRule databaseRule = new EmbeddedDatabaseRule();

    private volatile boolean stop;

    @Test
    @RepeatRule.Repeat( times = 5 )
    public void concurrentLabelTokenCreation() throws InterruptedException
    {
        int concurrentWorkers = 10;
        CountDownLatch latch = new CountDownLatch( concurrentWorkers );
        for ( int i = 0; i < concurrentWorkers; i++ )
        {
            new LabelCreator( databaseRule, latch ).start();
        }
        LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 500 ) );
        stop = true;
        latch.await();
    }

    public Label[] getLabels()
    {
        int randomLabelValue = ThreadLocalRandom.current().nextInt( 2 ) + 1;
        Label[] labels = new Label[randomLabelValue];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = Label.label( RandomStringUtils.randomAscii( randomLabelValue ) );
        }
        return labels;
    }

    private class LabelCreator extends Thread
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
                        Node node = database.createNode( createdLabels );
                        Iterable<Label> nodeLabels = node.getLabels();
                        assertEquals( asSet( asList( createdLabels ) ), asSet( nodeLabels ) );
                        transaction.success();
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
