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
package org.neo4j.graphdb.schema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.first;

@TestDirectoryExtension
class CancelIndexPopulationIT
{
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";

    @Inject
    private TestDirectory directory;

    @Test
    void shouldKeepIndexInPopulatingStateBetweenRestarts() throws InterruptedException, IOException
    {
        DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder( directory.homeDir() ).build();
        try
        {
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );

            // given
            Monitors monitors = db.getDependencyResolver().resolveDependency( Monitors.class );
            Barrier.Control barrier = new Barrier.Control();
            monitors.addMonitorListener( populationCompletionBlocker( barrier ) );

            // when
            createRelevantNode( db );
            createIndex( db );
            barrier.await();
        }
        finally
        {
            // This call to shutdown will eventually make a call to populationCancelled on the monitor below
            dbms.shutdown();
        }

        dbms = new TestDatabaseManagementServiceBuilder( directory.homeDir() ).build();
        try
        {
            GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );

            // then
            assertEquals( Schema.IndexState.ONLINE, awaitAndGetIndexState( db ) );
        }
        finally
        {
            dbms.shutdown();
        }
    }

    private Schema.IndexState awaitAndGetIndexState( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition indexDefinition = first( tx.schema().getIndexes( LABEL ) );
            tx.schema().awaitIndexOnline( indexDefinition, 1, TimeUnit.MINUTES );
            Schema.IndexState indexState = tx.schema().getIndexState( indexDefinition );
            tx.commit();
            return indexState;
        }
    }

    private void createIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( KEY ).create();
            tx.commit();
        }
    }

    private void createRelevantNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( LABEL ).setProperty( KEY, "value" );
            tx.commit();
        }
    }

    private IndexingService.MonitorAdapter populationCompletionBlocker( Barrier.Control barrier )
    {
        return new IndexingService.MonitorAdapter()
        {
            @Override
            public void indexPopulationScanComplete()
            {
                barrier.reached();
            }

            @Override
            public void populationCancelled()
            {
                // When we get this call we know that the population is still active (due to being blocked in indexPopulationScanComplete())
                // and have just gotten a call to being cancelled, which should now be known to index populators.
                barrier.release();
            }
        };
    }
}
