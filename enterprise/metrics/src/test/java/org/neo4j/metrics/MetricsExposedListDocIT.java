/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.neo4j.metrics.MetricsSettings.CsvFile.single;
import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.csvFile;
import static org.neo4j.metrics.MetricsSettings.csvPath;

public class MetricsExposedListDocIT
{

    @Rule
    public final TargetDirectory.TestDirectory folder = TargetDirectory.testDirForTest( getClass() );

    private GraphDatabaseService db;
    private File outputFile;

    @Before
    public void setup() throws IOException
    {
        String dbPath = folder.directory( "data" ).getAbsolutePath();
        outputFile = folder.file( "metrics.csv" );
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbPath ).
                setConfig( csvEnabled, Settings.TRUE ).
                setConfig( csvFile, single.name() ).
                setConfig( csvPath, outputFile.getAbsolutePath() ).newGraphDatabase();
    }

    @After
    public void shutdown()
    {
        db.shutdown();
    }


    @Test
    public void listExposedMetrics() throws Exception
    {
        // Create some activity that will generate some metrics data.
        for ( int i = 0; i < 1000; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Map<String,Object> params = MapUtil.map( "name", UUID.randomUUID().toString() );
                db.execute( "create (n:Label {name: {name}})", params );
                tx.success();
            }
        }

        // Read the metrics we recorded
        String[] headers;
        try ( BufferedReader reader = new BufferedReader( new FileReader( outputFile ) ) )
        {
            // First verify that some metrics have been written
            headers = reader.readLine().split( "," );
            assertThat( headers[0], is( "timestamp" ) );
            int committedColumn = Arrays.binarySearch( headers, "neo4j.transaction.committed" );
            assertThat( committedColumn, is( not( -1 ) ) );
        }

        // Now print them to the manual
        try ( PrintStream out = new PrintStream( file( "ops", "metrics-list.adoc" ) ) )
        {
            for ( String header : headers )
            {
                if ( !"timestamp".equals( header ) && !"datetime".equals( header ) )
                {
                    out.println( String.format( "* `%s`", header ) );
                }
            }
        }
    }

    private File file( String section, String name )
    {
        File directory = new File( new File( new File( "target" ), "docs" ), section );
        directory.mkdirs();
        return new File( directory, name );
    }
}
