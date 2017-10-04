/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

@Ignore( "These are rudimentary benchmarks, but implemented via the jUnit framework to make them easy to run " +
         "from an IDE." )
public class BloomFulltextIndexBenchmarks
{
    private static final String[] WORDS =
            ("dui nunc mattis enim ut tellus elementum sagittis vitae et leo duis ut diam quam nulla porttitor " +
             "massa id neque aliquam vestibulum morbi blandit cursus risus at ultrices mi tempus imperdiet nulla " +
             "malesuada pellentesque elit eget gravida cum sociis natoque penatibus et magnis dis parturient " +
             "montes nascetur ridiculus mus mauris").split( " " );

    @Rule
    public final FileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private TestGraphDatabaseFactory factory;
    private GraphDatabaseService db;

    private void createTestGraphDatabaseFactory()
    {
        factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
    }

    private void configureBloomExtension()
    {
        factory.addKernelExtensions( Collections.singletonList( new BloomKernelExtensionFactory() ) );
    }

    private static void clearAndCreateRandomSentence( ThreadLocalRandom rng, StringBuilder sb )
    {
        sb.setLength( 0 );
        int wordCount = rng.nextInt( 3, 7 );
        for ( int k = 0; k < wordCount; k++ )
        {
            sb.append( WORDS[rng.nextInt( WORDS.length )] ).append( ' ' );
        }
        sb.setLength( sb.length() - 1 );
    }

    @Test
    public void fiveHundredThousandOnlineUpdates() throws Exception
    {
        createTestGraphDatabaseFactory();
        configureBloomExtension();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( BloomFulltextConfig.bloom_indexed_properties, "prop" );
        db = builder.newGraphDatabase();

        int trials = 50;
        int threadCount = 10;
        int updatesPerThread = 1000;
        int updatesPerIteration = 10;
        int iterationsPerThread = updatesPerThread / updatesPerIteration;

        Runnable work = () ->
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            StringBuilder sb = new StringBuilder();
            for ( int i = 0; i < iterationsPerThread; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    for ( int j = 0; j < updatesPerIteration; j++ )
                    {
                        clearAndCreateRandomSentence( rng, sb );
                        db.createNode().setProperty( "prop", sb.toString() );
                    }
                    tx.success();
                }
            }
        };

        for ( int i = 0; i < trials; i++ )
        {
            long startMillis = System.currentTimeMillis();
            Thread[] threads = new Thread[threadCount];
            for ( int j = 0; j < threadCount; j++ )
            {
                threads[j] = new Thread( work );
            }
            for ( Thread thread : threads )
            {
                thread.start();
            }
            for ( Thread thread : threads )
            {
                thread.join();
            }
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            System.out.printf( "online update elapsed: %s ms.%n", elapsedMillis );
        }
    }

    @Test
    public void fiveHundredThousandNodesForPopulation() throws Exception
    {
        // First create the data
        createTestGraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        db = builder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            StringBuilder sb = new StringBuilder();
            for ( int i = 0; i < 500_000; i++ )
            {
                clearAndCreateRandomSentence( rng, sb );
                db.createNode().setProperty( "prop", sb.toString() );
            }
            tx.success();
        }
        db.shutdown();

        // Then measure startup performance
        configureBloomExtension();
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( BloomFulltextConfig.bloom_indexed_properties, "prop" );

        for ( int i = 0; i < 50; i++ )
        {
            long startMillis = System.currentTimeMillis();
            db = builder.newGraphDatabase();
            db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            System.out.printf( "startup populate elapsed: %s ms.%n", elapsedMillis );
            db.shutdown();
        }
        db = null;
    }

    @After
    public void after() throws Exception
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }
}
