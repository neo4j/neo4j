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
package schema;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.Randoms;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.test.DoubleLatch.awaitLatch;

public class DynamicIndexStoreViewIT
{
    private final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( suppressOutput );

    @Test
    public void populateDbWithConcurrentUpdates() throws Exception
    {
        GraphDatabaseService database =
                new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
        try
        {
            Randoms random = new Randoms();
            int counter = 1;
            for ( int j = 0; j < 100; j++ )
            {
                try ( Transaction transaction = database.beginTx() )
                {
                    for ( int i = 0; i < 5; i++ )
                    {
                        Node node = database.createNode( Label.label( "label" + counter ) );
                        node.setProperty( "property", MultipleIndexPopulationStressIT.randomPropertyValue( random ) );
                    }
                    transaction.success();
                }
                counter++;
            }

            int populatorCount = 5;
            ExecutorService executor = Executors.newFixedThreadPool( populatorCount );
            CountDownLatch startSignal = new CountDownLatch( 1 );
            AtomicBoolean endSignal = new AtomicBoolean();
            for ( int i = 0; i < populatorCount; i++ )
            {
                executor.submit( new Populator( database, counter, startSignal, endSignal ) );
            }

            try
            {
                try ( Transaction transaction = database.beginTx() )
                {
                    database.schema().indexFor( Label.label( "label10" ) ).on( "property" ).create();
                    transaction.success();
                }
                startSignal.countDown();

                try ( Transaction transaction = database.beginTx() )
                {
                    database.schema().awaitIndexesOnline( populatorCount, TimeUnit.MINUTES );
                    transaction.success();
                }
            }
            finally
            {
                endSignal.set( true );
                executor.shutdown();
                // Basically we don't care to await their completion because they've done their job
            }
        }
        finally
        {
            database.shutdown();
            ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
            Config config = Config.defaults(  GraphDatabaseSettings.pagecache_memory, "8m" );
            consistencyCheckService.runFullConsistencyCheck( testDirectory.graphDbDir(), config,
                    ProgressMonitorFactory.NONE, FormattedLogProvider.toOutputStream( System.out ), false );
        }
    }

    private class Populator implements Runnable
    {
        private final GraphDatabaseService databaseService;
        private final long totalNodes;
        private final CountDownLatch startSignal;
        private final AtomicBoolean endSignal;

        Populator( GraphDatabaseService databaseService, long totalNodes, CountDownLatch startSignal,
                AtomicBoolean endSignal )
        {
            this.databaseService = databaseService;
            this.totalNodes = totalNodes;
            this.startSignal = startSignal;
            this.endSignal = endSignal;
        }

        @Override
        public void run()
        {
            Randoms random = new Randoms();
            awaitLatch( startSignal );
            while ( !endSignal.get() )
            {
                try ( Transaction transaction = databaseService.beginTx() )
                {
                    try
                    {
                        int operationType = random.nextInt( 3 );
                        switch ( operationType )
                        {
                        case 0:
                            long targetNodeId = random.nextLong( totalNodes );
                            databaseService.getNodeById( targetNodeId ).delete();
                            break;
                        case 1:
                            long nodeId = random.nextLong( totalNodes );
                            Node node = databaseService.getNodeById( nodeId );
                            Map<String,Object> allProperties = node.getAllProperties();
                            for ( String key : allProperties.keySet() )
                            {
                                node.setProperty( key, MultipleIndexPopulationStressIT.randomPropertyValue( random ) );
                            }
                            break;
                        case 2:
                            Node nodeToUpdate = databaseService.createNode( Label.label( "label10" ) );
                            nodeToUpdate.setProperty( "property", MultipleIndexPopulationStressIT.randomPropertyValue( random ) );
                            break;
                        default:
                            throw new UnsupportedOperationException( "Unknown type of index operation" );
                        }
                        transaction.success();
                    }
                    catch ( Exception e )
                    {
                        transaction.failure();
                    }
                }
            }
        }
    }
}
