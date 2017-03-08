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
package org.neo4j.metrics;


import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.metrics.source.db.PageCacheMetrics;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class PageCacheMetricsIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    private File metricsDirectory;
    private GraphDatabaseService database;

    @Before
    public void setUp()
    {
        metricsDirectory = testDirectory.directory( "metrics" );
        database = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( MetricsSettings.neoPageCacheEnabled, Settings.TRUE  )
                .setConfig( MetricsSettings.csvEnabled, Settings.TRUE )
                .setConfig( MetricsSettings.csvInterval, "100ms" )
                .setConfig( MetricsSettings.csvPath, metricsDirectory.getAbsolutePath() ).newGraphDatabase();
    }

    @After
    public void tearDown()
    {
        database.shutdown();
    }

    @Test
    public void pageCacheMetrics() throws Exception
    {
        Label testLabel = Label.label( "testLabel" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( testLabel );
            node.setProperty( "property", "value" );
            transaction.success();
        }

        try ( Transaction ignored = database.beginTx() )
        {
            ResourceIterator<Node> nodes = database.findNodes( testLabel );
            Assert.assertEquals( 1, nodes.stream().count() );
        }
        assertMetrics( "Page cache pins should be included in metrics report.", PageCacheMetrics.PC_PINS, greaterThan( 0L ) );
        assertMetrics( "Page cache unpins should be included in metrics report.", PageCacheMetrics.PC_UNPINS, greaterThan( 0L ) );
        assertMetrics( "Page cache evictions should be included in metrics report.", PageCacheMetrics.PC_EVICTIONS, equalTo( 0L ) );
        assertMetrics( "Page cache page faults should be included in metrics report.", PageCacheMetrics.PC_PAGE_FAULTS, greaterThan( 0L ) );
        assertMetrics( "Page cache hits should be included in metrics report.", PageCacheMetrics.PC_HITS, greaterThan( 0L ) );
        assertMetrics( "Page cache flushes should be included in metrics report.", PageCacheMetrics.PC_FLUSHES, greaterThanOrEqualTo( 0L ) );
        assertMetrics( "Page cache exceptions should be included in metrics report.", PageCacheMetrics.PC_EVICTION_EXCEPTIONS, equalTo( 0L ) );
    }

    private void assertMetrics( String message, String metricName, Matcher<Long> matcher ) throws Exception
    {
        assertEventually( message, () -> readLongValue( metricsCsv( metricsDirectory, metricName ) ), matcher, 5, SECONDS );
    }

}
