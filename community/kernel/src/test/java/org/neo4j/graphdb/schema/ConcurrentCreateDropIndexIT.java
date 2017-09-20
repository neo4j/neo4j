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

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.Race;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.Iterables.asList;

public class ConcurrentCreateDropIndexIT
{
    private static final String KEY = "key";

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldTest() throws Throwable
    {
        // GIVEN all the required labels created, to not disturb timings in racing threads later
        int threads = Runtime.getRuntime().availableProcessors();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < threads; i++ )
            {
                db.createNode( label( i ) );
            }
            tx.success();
        }

        // WHEN concurrently creating indexes for different labels
        Race race = new Race();
        for ( int i = 0; i < threads; i++ )
        {
            int yo = i;
            race.addContestant( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.schema().indexFor( label( yo ) ).on( KEY ).create();
                    tx.success();
                }
            }, 1 );
        }
        race.go();

        // THEN they should all be observed as existing in the end
        List<IndexDefinition> indexes;
        try ( Transaction tx = db.beginTx() )
        {
            indexes = asList( db.schema().getIndexes() );
            tx.success();
        }
        assertEquals( threads, indexes.size() );

        // and WHEN dropping them
        race = new Race();
        for ( IndexDefinition index : indexes )
        {
            race.addContestant( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    index.drop();
                    tx.success();
                }
            }, 1 );
        }
        race.go();

        // THEN they should all be observed as dropped in the end
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 0, asList( db.schema().getIndexes() ).size() );
            tx.success();
        }
    }

    private static Label label( int i )
    {
        return Label.label( "L" + i );
    }
}
