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
package org.neo4j.cypher;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.test.EmbeddedDatabaseRule;

import static java.lang.String.format;

@Ignore("Too costly to run by default but useful for testing resource clean up and indexing")
public class ManyMergesStressTest
{
    private Random random = new Random();

    private String[] SYLLABLES = new String[] { "Om", "Pa", "So", "Hu", "Ma", "Ni", "Ru", "Gu", "Ha", "Ta" };

    private final static int TRIES = 8000;

    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule();

    @Test
    public void shouldWorkFine() throws IOException
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseService();

        Label person = DynamicLabel.label( "Person" );

        try ( Transaction tx = db.beginTx() )
        {
            // THIS USED TO CAUSE OUT OF FILE HANDLES
            // (maybe look at:  http://stackoverflow.com/questions/6210348/too-many-open-files-error-on-lucene)
            db.schema().indexFor( person ).on( "id" ).create();

            // THIS SHOULD ALSO WORK
            db.schema().constraintFor( person ).assertPropertyIsUnique( "id" ).create();

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( person ).on( "name" ).create();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }

        org.neo4j.cypher.javacompat.ExecutionEngine engine = new org.neo4j.cypher.javacompat.ExecutionEngine( db );

        for( int count = 0; count < TRIES; count++ )
        {
            Pair<String, String> stringPair = getRandomName();
            String ident = stringPair.first();
            String name = stringPair.other();
            String id = Long.toString( Math.abs( random.nextLong() ) );
            String query =
                format( "MERGE (%s:Person {id: %s}) ON CREATE SET %s.name = \"%s\";", ident, id, ident, name );

            ExecutionResult result = engine.execute( query );
            result.iterator().close();
        }
    }

    public Pair<String, String> getRandomName()
    {
        StringBuilder identBuilder = new StringBuilder();
        StringBuilder nameBuilder = new StringBuilder();

        for ( int j = 0; j < 10; j++ )
        {
            String part = SYLLABLES[random.nextInt( SYLLABLES.length )];
            if ( j != 0 )
            {
                identBuilder.append( '_' );
                nameBuilder.append( ' ' );
            }
            identBuilder.append( part );
            nameBuilder.append( part );
        }

        return Pair.of( identBuilder.toString(), nameBuilder.toString() );
    }
}
