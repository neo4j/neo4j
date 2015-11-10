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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.metrics.source.CypherMetrics;
import org.neo4j.metrics.source.TransactionMetrics;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cypher_min_replan_interval;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.csvPath;
import static org.neo4j.metrics.MetricsSettings.metricsEnabled;

public class MetricsKernelExtensionFactoryIT
{
    @Rule
    public final TargetDirectory.TestDirectory folder = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    private File outputPath;
    private ClusterManager.ManagedCluster cluster;
    private HighlyAvailableGraphDatabase db;

    @Before
    public void setup() throws Exception
    {
        outputPath = folder.file( "metrics.csv" );
        Map<String,String> config = new HashMap<>();
        config.put( MetricsSettings.neoEnabled.name(), Settings.TRUE );
        config.put( metricsEnabled.name(), Settings.TRUE );
        config.put( csvEnabled.name(), Settings.TRUE );
        config.put( cypher_min_replan_interval.name(), "0" );
        config.put( csvPath.name(), outputPath.getAbsolutePath() );
        cluster = clusterRule.withSharedConfig( config ).withProvider( clusterOfSize( 1 ) ).startCluster();
        db = cluster.getMaster();
    }

    @Test
    public void mustLoadMetricsExtensionWhenConfigured() throws Throwable
    {
        // Create some activity that will show up in the metrics data.
        addNodes( 1000 );
        cluster.stop();

        // Awesome. Let's get some metric numbers.
        // We should at least have a "timestamp" column, and a "neo4j.transaction.committed" column
        File metricsFile = new File( outputPath, TransactionMetrics.TX_COMMITTED + ".csv" );
        try ( BufferedReader reader = new BufferedReader( new FileReader( metricsFile ) ) )
        {
            String[] headers = reader.readLine().split( "," );
            assertThat( headers[0], is( "t" ) );
            assertThat( headers[1], is( "value" ) );

            // Now we can verify that the number of committed transactions should never decrease.
            int committedTransactions = 0;
            String line;
            while ( (line = reader.readLine()) != null )
            {
                String[] fields = line.split( "," );
                int newCommittedTransactions = Integer.parseInt( fields[1] );
                assertThat( newCommittedTransactions, greaterThanOrEqualTo( committedTransactions ) );
                committedTransactions = newCommittedTransactions;
            }
        }
    }

    @Test
    public void showReplanEvents() throws Throwable
    {
        //do a simple query to populate cache
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "match (n:Label {name: 'Pontus'}) return n.name" );
            tx.success();
        }
        //add some data, should make plan stale
        addNodes( 10 );
        //now we should have to plan again
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "match (n:Label {name: 'Pontus'}) return n.name" );
            tx.success();
        }
        //add more data just to make sure metrics are flushed
        addNodes( 1000 );
        cluster.stop();

        //now we should have one replan event
        File metricFile = new File( outputPath, CypherMetrics.REPLAN_EVENTS + ".csv" );
        try ( BufferedReader reader = new BufferedReader( new FileReader( metricFile ) ) )
        {
            String[] headers = reader.readLine().split( "," );
            int replanColumn = Arrays.binarySearch( headers, "value" );
            assertThat( replanColumn, is( not( -1 ) ) );

            // Now we can verify that the number of committed transactions should never decrease.
            int replanEvents = 0;
            String line;
            while ( (line = reader.readLine()) != null )
            {
                String[] fields = line.split( "," );
                int newReplanEvents = Integer.parseInt( fields[replanColumn] );
                assertThat( newReplanEvents, greaterThanOrEqualTo( replanEvents ) );
                replanEvents = newReplanEvents;
            }

            assertThat( replanEvents, is( 1 ) );
        }
    }

    private void addNodes( int numberOfNodes )
    {
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( Label.label( "Label" ) );
                node.setProperty( "name", UUID.randomUUID().toString() );
                tx.success();
            }
        }
    }
}
