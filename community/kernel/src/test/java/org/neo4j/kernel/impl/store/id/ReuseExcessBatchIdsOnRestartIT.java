/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.id;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import javax.annotation.Resource;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.EmbeddedDatabaseExtension;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.lang.Integer.parseInt;
import static java.lang.Math.toIntExact;
import static java.lang.Runtime.getRuntime;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_id_batch_size;

@ExtendWith( EmbeddedDatabaseExtension.class )
public class ReuseExcessBatchIdsOnRestartIT
{
    @Resource
    public EmbeddedDatabaseRule db;

    // Knowing that ids are grabbed in batches internally we only create one node and later assert
    // that the excess ids that were only grabbed, but not used can be reused.
    @Test
    public void shouldReuseExcessBatchIdsWhichWerentUsedBeforeClose() throws Exception
    {
        // given
        Node firstNode;
        try ( Transaction tx = db.beginTx() )
        {
            firstNode = db.createNode();
            tx.success();
        }

        // when
        db.restartDatabase();

        Node secondNode;
        try ( Transaction tx = db.beginTx() )
        {
            secondNode = db.createNode();
            tx.success();
        }

        // then
        assertEquals( firstNode.getId() + 1, secondNode.getId() );
    }

    @Test
    public void shouldBeAbleToReuseAllIdsInConcurrentCommitsWithRestart()
    {
        assertTimeout( ofMillis( 30_000 ), () -> {
            //  given

            int threads = getRuntime().availableProcessors();
            int batchSize = parseInt( record_id_batch_size.getDefaultValue() );
            ExecutorService executor = newFixedThreadPool( threads );
            AtomicIntegerArray createdIds = new AtomicIntegerArray( threads * batchSize );
            for ( int i = 0; i < threads; i++ )
            {
                executor.submit( () -> {
                    try ( Transaction tx = db.beginTx() )
                    {
                        for ( int j = 0; j < batchSize / 2; j++ )
                        {
                            int index = toIntExact( db.createNode().getId() );
                            createdIds.set( index, 1 );
                        }
                        tx.success();
                    }
                } );
            }
            executor.shutdown();
            while ( !executor.awaitTermination( 1, SECONDS ) )
            {   // Just wait longer
            }
            assertFalse( allSet( createdIds ) );

            // when/then
            db.restartDatabase();
            try ( Transaction tx = db.beginTx() )
            {
                while ( !allSet( createdIds ) )
                {
                    int index = toIntExact( db.createNode().getId() );
                    assert createdIds.get( index ) != 1;
                    createdIds.set( index, 1 );
                }
                tx.success();
            }
        } );
    }

    private static boolean allSet( AtomicIntegerArray values )
    {
        for ( int i = 0; i < values.length(); i++ )
        {
            if ( values.get( i ) == 0 )
            {
                return false;
            }
        }
        return true;
    }
}
