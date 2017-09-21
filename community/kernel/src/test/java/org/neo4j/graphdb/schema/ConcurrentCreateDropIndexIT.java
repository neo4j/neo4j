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
package org.neo4j.graphdb.schema;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.Race;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.asList;

public class ConcurrentCreateDropIndexIT
{
    private static final String KEY = "key";

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    private final int threads = Runtime.getRuntime().availableProcessors();

    @Before
    public void createTokens()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < threads; i++ )
            {
                db.createNode( label( i ) ).setProperty( KEY, i );
            }
            tx.success();
        }
    }

    @Test
    public void concurrentCreatingOfIndexesShouldNotInterfere() throws Throwable
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
            List<IndexDefinition> indexes = asList( db.schema().getIndexes() );
            assertEquals( threads, indexes.size() );
            Set<String> labels = new HashSet<>();
            for ( IndexDefinition index : indexes )
            {
                assertTrue( labels.add( index.getLabel().name() ) );
            }
            tx.success();
        }
    }

    @Test
    public void concurrentDroppingOfIndexesShouldNotInterfere() throws Throwable
    {
        // GIVEN created indexes
        List<IndexDefinition> indexes = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < threads; i++ )
            {
                indexes.add( db.schema().indexFor( label( i ) ).on( KEY ).create() );
            }
            tx.success();
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
            assertEquals( 0, asList( db.schema().getIndexes() ).size() );
            tx.success();
        }
    }

    @Test
    public void concurrentMixedCreatingAndDroppingOfIndexesShouldNotInterfere() throws Throwable
    {
        // GIVEN created indexes
        List<IndexDefinition> indexesToDrop = new ArrayList<>();
        int creates = threads / 2;
        int drops = threads - creates;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < drops; i++ )
            {
                indexesToDrop.add( db.schema().indexFor( label( i ) ).on( KEY ).create() );
            }
            tx.success();
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
            List<IndexDefinition> indexes = asList( db.schema().getIndexes() );
            assertEquals( creates, indexes.size() );
            tx.success();

            for ( IndexDefinition index : indexes )
            {
                assertTrue( expectedIndexedLabels.remove( index.getLabel().name() ) );
            }
        }
    }

    private Runnable indexCreate( int labelIndex )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( label( labelIndex ) ).on( KEY ).create();
                tx.success();
            }
        };
    }

    private Runnable indexDrop( IndexDefinition index )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                index.drop();
                tx.success();
            }
        };
    }

    private static Label label( int i )
    {
        return Label.label( "L" + i );
    }
}
