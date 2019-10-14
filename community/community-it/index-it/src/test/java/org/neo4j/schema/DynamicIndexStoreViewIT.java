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
package org.neo4j.schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.values.storable.RandomValues;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.DoubleLatch.awaitLatch;

@Neo4jLayoutExtension
@ExtendWith( SuppressOutputExtension.class )
public class DynamicIndexStoreViewIT
{
    @Inject
    private SuppressOutput suppressOutput;

    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void populateDbWithConcurrentUpdates() throws Exception
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            RandomValues randomValues = RandomValues.create();
            int counter = 1;
            for ( int j = 0; j < 100; j++ )
            {
                try ( Transaction transaction = database.beginTx() )
                {
                    for ( int i = 0; i < 5; i++ )
                    {
                        Node node = transaction.createNode( Label.label( "label" + counter ) );
                        node.setProperty( "property", randomValues.nextValue().asObject() );
                    }
                    transaction.commit();
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
                    transaction.schema().indexFor( Label.label( "label10" ) ).on( "property" ).create();
                    transaction.commit();
                }
                startSignal.countDown();

                try ( Transaction transaction = database.beginTx() )
                {
                    transaction.schema().awaitIndexesOnline( populatorCount, TimeUnit.MINUTES );
                    transaction.commit();
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
            managementService.shutdown();
            ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService();
            Config config = Config.defaults(  GraphDatabaseSettings.pagecache_memory, "8m" );
            consistencyCheckService.runFullConsistencyCheck( databaseLayout, config,
                    ProgressMonitorFactory.NONE, FormattedLogProvider.toOutputStream( System.out ), false );
        }
    }

    private static class Populator implements Runnable
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
            RandomValues randomValues = RandomValues.create();
            awaitLatch( startSignal );
            while ( !endSignal.get() )
            {
                try ( Transaction transaction = databaseService.beginTx() )
                {
                    try
                    {
                        int operationType = randomValues.nextIntValue( 3 ).value();
                        switch ( operationType )
                        {
                        case 0:
                            long targetNodeId = randomValues.nextLongValue( totalNodes ).value();
                            transaction.getNodeById( targetNodeId ).delete();
                            break;
                        case 1:
                            long nodeId = randomValues.nextLongValue( totalNodes ).value();
                            Node node = transaction.getNodeById( nodeId );
                            Map<String,Object> allProperties = node.getAllProperties();
                            for ( String key : allProperties.keySet() )
                            {
                                node.setProperty( key, randomValues.nextValue().asObject() );
                            }
                            break;
                        case 2:
                            Node nodeToUpdate = transaction.createNode( Label.label( "label10" ) );
                            nodeToUpdate.setProperty( "property", randomValues.nextValue().asObject()  );
                            break;
                        default:
                            throw new UnsupportedOperationException( "Unknown type of index operation" );
                        }
                        transaction.commit();
                    }
                    catch ( Exception e )
                    {
                        transaction.rollback();
                    }
                }
            }
        }
    }
}
