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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterables.singleOrNull;

@ImpermanentDbmsExtension
class ConcurrentCreateDropIndexIT
{
    private final int threads = Runtime.getRuntime().availableProcessors();
    private static final String KEY = "key";
    @Inject
    private GraphDatabaseAPI db;

    @BeforeEach
    void createTokens()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < threads; i++ )
            {
                tx.createNode( label( i ) ).setProperty( KEY, i );
            }
            tx.commit();
        }
    }

    @Test
    void concurrentCreatingOfIndexesShouldNotInterfere() throws Throwable
    {
        // WHEN concurrently creating indexes for different labels
        Race race = new Race();
        for ( int i = 0; i < threads; i++ )
        {
            race.addContestant( indexCreate( i ), 1 );
        }
        race.go();

        // THEN they should all be observed as existing in the end
        try ( Transaction tx = db.beginTx() )
        {
            List<IndexDefinition> indexes = asList( tx.schema().getIndexes() );
            assertEquals( threads, indexes.size() );
            Set<String> labels = new HashSet<>();
            for ( IndexDefinition index : indexes )
            {
                assertTrue( labels.add( single( index.getLabels() ).name() ) );
            }
            tx.commit();
        }
    }

    @Test
    void concurrentDroppingOfIndexesShouldNotInterfere() throws Throwable
    {
        // GIVEN created indexes
        List<IndexDefinition> indexes = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < threads; i++ )
            {
                indexes.add( tx.schema().indexFor( label( i ) ).on( KEY ).create() );
            }
            tx.commit();
        }

        // WHEN dropping them
        Race race = new Race();
        for ( IndexDefinition index : indexes )
        {
            race.addContestant( indexDrop( index ), 1 );
        }
        race.go();

        // THEN they should all be observed as dropped in the end
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 0, asList( tx.schema().getIndexes() ).size() );
            tx.commit();
        }
    }

    @Test
    void concurrentMixedCreatingAndDroppingOfIndexesShouldNotInterfere() throws Throwable
    {
        // GIVEN created indexes
        List<IndexDefinition> indexesToDrop = new ArrayList<>();
        int creates = threads / 2;
        int drops = threads - creates;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < drops; i++ )
            {
                indexesToDrop.add( tx.schema().indexFor( label( i ) ).on( KEY ).create() );
            }
            tx.commit();
        }

        // WHEN dropping them
        Race race = new Race();
        Set<String> expectedIndexedLabels = new HashSet<>();
        for ( int i = 0; i < creates; i++ )
        {
            expectedIndexedLabels.add( label( drops + i ).name() );
            race.addContestant( indexCreate( drops + i ), 1 );
        }
        for ( IndexDefinition index : indexesToDrop )
        {
            race.addContestant( indexDrop( index ), 1 );
        }
        race.go();

        // THEN they should all be observed as dropped in the end
        try ( Transaction tx = db.beginTx() )
        {
            List<IndexDefinition> indexes = asList( tx.schema().getIndexes() );
            assertEquals( creates, indexes.size() );

            for ( IndexDefinition index : indexes )
            {
                assertTrue( expectedIndexedLabels.remove( single( index.getLabels() ).name() ) );
            }
            tx.commit();
        }
    }

    @Test
    void concurrentCreatingUniquenessConstraint() throws Throwable
    {
        // given
        Race race = new Race().withMaxDuration( 10, SECONDS );
        Label label = label( 0 );
        race.addContestants( 10, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().constraintFor( label ).assertPropertyIsUnique( KEY ).create();
                tx.commit();
            }
            catch ( TransientFailureException | ConstraintViolationException e )
            {   // It's OK
            }
        }, 300 );

        // when
        race.go();

        try ( Transaction tx = db.beginTx() )
        {
            // then
            ConstraintDefinition constraint = single( tx.schema().getConstraints( label ) );
            assertNotNull( constraint );
            IndexDefinition index = single( tx.schema().getIndexes( label ) );
            assertNotNull( index );
            tx.commit();
        }
    }

    @Test
    void concurrentCreatingUniquenessConstraintOnNonUniqueData() throws Throwable
    {
        // given
        Label label = label( 0 );
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 2; i++ )
            {
                tx.createNode( label ).setProperty( KEY, "A" );
            }
            tx.commit();
        }
        Race race = new Race().withMaxDuration( 10, SECONDS );
        race.addContestants( 3, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().constraintFor( label ).assertPropertyIsUnique( KEY ).create();
                tx.commit();
            }
            catch ( TransientFailureException | ConstraintViolationException e )
            {   // It's OK
            }
        }, 100 );

        // when
        race.go();

        try ( Transaction tx = db.beginTx() )
        {
            // then
            ConstraintDefinition constraint = singleOrNull( tx.schema().getConstraints( label ) );
            assertNull( constraint );
            IndexDefinition index = singleOrNull( tx.schema().getIndexes( label ) );
            assertNull( index );
            tx.commit();
        }
    }

    @Test
    void concurrentCreatingAndAwaitingIndexesOnline() throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> indexCreate = executor.submit( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().indexFor( label( 0 ) ).on( KEY ).create();
                    tx.commit();
                }
            } );
            while ( !indexCreate.isDone() )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                    tx.commit();
                }
            }
            indexCreate.get();
        }
        finally
        {
            executor.shutdown();
        }
    }

    private Runnable indexCreate( int labelIndex )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().indexFor( label( labelIndex ) ).on( KEY ).create();
                tx.commit();
            }
        };
    }

    private Runnable indexDrop( IndexDefinition index )
    {
        return () ->
        {
            while ( true )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().getIndexByName( index.getName() ).drop();
                    tx.commit();
                    return;
                }
                catch ( Exception dde )
                {
                    //ignored
                }
            }
        };
    }

    private static Label label( int i )
    {
        return Label.label( "L" + i );
    }
}
