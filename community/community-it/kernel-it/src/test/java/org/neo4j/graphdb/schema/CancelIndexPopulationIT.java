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

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.first;

public class CancelIndexPopulationIT
{
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";

    @Rule
    public final EmbeddedDatabaseRule db = new EmbeddedDatabaseRule();

    @Test
    public void shouldKeepIndexInPopulatingStateBetweenRestarts() throws InterruptedException, IOException
    {
        // given
        Monitors monitors = db.getDependencyResolver().resolveDependency( Monitors.class );
        Barrier.Control barrier = new Barrier.Control();
        monitors.addMonitorListener( populationCompletionBlocker( barrier ) );

        // when
        createRelevantNode();
        createIndex();
        barrier.await();
        // This call will eventually make a call to populationCancelled on the monitor below
        db.restartDatabase();

        // then
        assertEquals( Schema.IndexState.ONLINE, awaitAndGetIndexState() );
    }

    private Schema.IndexState awaitAndGetIndexState()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition indexDefinition = first( db.schema().getIndexes( LABEL ) );
            db.schema().awaitIndexOnline( indexDefinition, 1, TimeUnit.MINUTES );
            Schema.IndexState indexState = db.schema().getIndexState( indexDefinition );
            tx.success();
            return indexState;
        }
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.success();
        }
    }

    private void createRelevantNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL ).setProperty( KEY, "value" );
            tx.success();
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
