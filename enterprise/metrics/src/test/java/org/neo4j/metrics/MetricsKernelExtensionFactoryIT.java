/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.metrics;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.metrics.MetricsSettings.CsvFile.single;
import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.csvFile;
import static org.neo4j.metrics.MetricsSettings.csvPath;

public class MetricsKernelExtensionFactoryIT
{
    @Rule
    public final TargetDirectory.TestDirectory folder = TargetDirectory.testDirForTest( getClass() );

    GraphDatabaseService db;
    private File outputFile;
    private File dbPath;

    @Before
    public void setup() throws IOException
    {
        dbPath = folder.directory( "data" );
        outputFile = folder.file( "metrics.csv" );
    }

    @After
    public void shutdown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void mustLoadMetricsExtensionWhenConfigured() throws Exception
    {
        // Start the database
        GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbPath );
        db = builder.setConfig( MetricsSettings.neoEnabled, Settings.TRUE )
                    .setConfig( csvEnabled, Settings.TRUE )
                    .setConfig( csvFile, single.name() )
                    .setConfig( csvPath, outputFile.getAbsolutePath() )
                    .newGraphDatabase();

        // Create some activity that will show up in the metrics data.
        for ( int i = 0; i < 1000; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Map<String,Object> params = MapUtil.map( "name", UUID.randomUUID().toString() );
                db.execute( "create (n:Label {name: {name}})", params );
                tx.success();
            }
        }

        // Awesome. Let's get some metric numbers.
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        try ( BufferedReader reader = new BufferedReader( new FileReader( outputFile ) ) )
        {
            String[] headers = reader.readLine().split( "," );
            assertThat( headers[0], is( "timestamp" ) );
            int committedColumn = Arrays.binarySearch( headers, "neo4j.transaction.committed" );
            assertThat( committedColumn, is( not( -1 ) ) );

            // Now we can verify that the number of committed transactions should never decrease.
            int committedTransactions = 0;
            String line;
            while ( (line = reader.readLine()) != null )
            {
                String[] fields = line.split( "," );
                int newCommittedTransactions = Integer.parseInt( fields[committedColumn] );
                assertThat( newCommittedTransactions, greaterThanOrEqualTo( committedTransactions ) );
                committedTransactions = newCommittedTransactions;
            }
        }
    }

    @Test
    public void mustBeAbleToStartWithNullTracer() throws Exception
    {
        // Start the database
        GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbPath );
        db = builder.setConfig( MetricsSettings.neoEnabled, Settings.TRUE )
                    .setConfig( csvEnabled, Settings.TRUE )
                    .setConfig( csvFile, single.name() )
                    .setConfig( csvPath, outputFile.getAbsolutePath() )
                    .setConfig( GraphDatabaseFacadeFactory.Configuration.tracer, "null" ) // key point!
                    .newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "all", "is well" );
            tx.success();
        }

        // We assert that no exception is thrown during startup or the operation of the database.
    }
}
