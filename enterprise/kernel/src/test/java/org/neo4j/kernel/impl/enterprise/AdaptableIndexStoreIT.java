/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.enterprise;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.TargetDirectory;

public class AdaptableIndexStoreIT
{

    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void populateDb() throws Exception
    {
        GraphDatabaseService database =
                new GraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
        int counter = 1;
        for ( int j = 0; j < 10000; j++ )
        {
            try ( Transaction transaction = database.beginTx() )
            {
                for ( int i = 0; i < 5; i++ )
                {
                    Node node = database.createNode( Label.label( "label" + counter ) );
                    node.setProperty( "property" , ThreadLocalRandom.current().nextInt() );
                }
                transaction.success();
            }
            counter++;
        }

        List<Populator> populators = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            Populator populator = new Populator( database, counter );
            populators.add( populator );
            populator.start();
        }


        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().indexFor( Label.label( "label10" ) ).on( "property" ).create();
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
            transaction.success();
        }

        populators.forEach( Populator::terminate );
        database.shutdown();

        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
        consistencyCheckService.runFullConsistencyCheck( testDirectory.graphDbDir(), Config.empty(),
                ProgressMonitorFactory.NONE, FormattedLogProvider.toOutputStream( System.out ), false );
    }

    private class Populator extends Thread
    {

        private GraphDatabaseService databaseService;
        private long totalNodes;
        private volatile boolean terminate;

        public Populator( GraphDatabaseService databaseService, long totalNodes )
        {
            this.databaseService = databaseService;
            this.totalNodes = totalNodes;
        }

        @Override
        public void run()
        {
            while ( !terminate )
            {
                try ( Transaction transaction = databaseService.beginTx() )
                {
                    try
                    {
                        int operationType = ThreadLocalRandom.current().nextInt( 3 );
                        switch ( operationType )
                        {
                        case 0:
                            long targetNodeId = ThreadLocalRandom.current().nextLong( totalNodes );
                            databaseService.getNodeById( targetNodeId ).delete();
                            break;
                        case 1:
                            long nodeId = ThreadLocalRandom.current().nextLong( totalNodes );
                            Node node = databaseService.getNodeById( nodeId );
                            Map<String,Object> allProperties = node.getAllProperties();
                            for ( String key : allProperties.keySet() )
                            {
                                node.setProperty( key, RandomStringUtils.random( 10 ) );
                            }
                            break;
                        case 2:
                            Node nodeToUpdate = databaseService.createNode( Label.label( "label10" ) );
                            nodeToUpdate.setProperty( "property", RandomStringUtils.random( 5 ) );
                            break;
                        default:
                            throw new UnsupportedOperationException( "Unknown type of index operation" );
                        }
                        transaction.success();
                    }
                    catch ( Exception e )
                    {
//                        e.printStackTrace(System.out);
                    }
                }
            }
        }

        public void terminate()
        {
            terminate = true;
        }
    }
}
