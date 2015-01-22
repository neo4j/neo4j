/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.index;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lookup;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.Integer.getInteger;
import static java.lang.System.nanoTime;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.count;

public class IndexSearchBenchmark
{
    private static final String[] PREFIXES;
    private static final int LOG_START_SIZE = getInt( "LOG_START_SIZE", 10 );
    private static final int TIMES = getInt( "TIMES", 5 );
    private static final int ITERATIONS = getInt( "ITERATIONS", 100 );
    private static final Label LABEL = label( "Person" );
    private static final String KEY = "name";

    static
    {
        PREFIXES = new String[getInt( "ITERATIONS", 10 )];
        for ( int i = 0; i < PREFIXES.length; i++ )
        {
            PREFIXES[i] = randomString( ThreadLocalRandom.current(), 5, 'A', 'Z' );
        }
    }

    private static String randomString( Random random, int length, char lower, char upper )
    {
        StringBuilder string = new StringBuilder( length );
        int range = upper - lower;
        for ( int i = 0; i < length; i++ )
        {
            string.append( (char) (lower + random.nextInt( range )) );
        }
        return string.toString();
    }

    public static void main( String... args ) throws Exception
    {
        for ( int i = 0; i < PREFIXES.length; i++ )
        {
            GraphDatabaseService db = createDatabase( LOG_START_SIZE + i );
            try
            {
                benchmark( db, i );
            }
            finally
            {
                db.shutdown();
            }
        }
    }

    private static void benchmark( GraphDatabaseService db, int i )
    {
        int logSize = LOG_START_SIZE + i;
        for ( int times = 0; times < TIMES; times++ )
        {
            for ( int p = 0; p < i; p++ )
            {
                String prefix = PREFIXES[p];
                long time = nanoTime();
                try ( Transaction tx = db.beginTx() )
                {
                    for ( int iterations = 0; iterations < ITERATIONS; iterations++ )
                    {
                        try ( ResourceIterator<?> hits = db.findNodes( LABEL, KEY, Lookup.startsWith( prefix ) ) )
                        {
                            count( hits );
                        }
                    }
                    tx.success();
                }
                time = nanoTime() - time;
                int hits = 1<< ((LOG_START_SIZE + p) / 2);
                double iterationTime = time / 1000000.0 / ITERATIONS;
                System.out.printf( "size = %d; hits = %d - %.3fms / search (%.3fms / hit)%n",
                                   1 << logSize, hits, iterationTime, iterationTime/hits );
            }
        }
    }

    private static GraphDatabaseService createDatabase( int logSize )
    {
        Random random = ThreadLocalRandom.current();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try
        {
            createIndex( db );
            for ( int i = LOG_START_SIZE, size = 1 << i; i < logSize; size = 1 << ++i )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    String prefix = PREFIXES[i - LOG_START_SIZE];
                    for ( int n = 0; n < size; n++ )
                    {
                        if ( random.nextBoolean() )
                        {
                            db.createNode( LABEL ).setProperty( KEY, prefix + randomString( random, 5, 'a', 'z' ) );
                        }
                        else
                        {
                            db.createNode( LABEL ).setProperty( KEY, randomString( random, 5, 'a', 'z' ) );
                        }
                    }
                    tx.success();
                }
            }
        }
        catch ( Throwable failure )
        {
            db.shutdown();
            throw failure;
        }
        return db;
    }

    private static void createIndex( GraphDatabaseService db )
    {
        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private static int getInt( String name, int defaultValue )
    {
        return getInteger( IndexSearchBenchmark.class.getName() + "." + name, defaultValue );
    }
}
