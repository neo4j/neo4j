/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.bolt.v1.messaging.message.Messages;
import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.metrics.source.db.BoltMetrics;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.acceptedVersions;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.chunk;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.test.Assert.assertEventually;

public class BoltMetricsIT
{
    @Rule public TemporaryFolder tmpDir = new TemporaryFolder();

    private GraphDatabaseAPI db;
    private Connection conn;

    @Test
    public void shouldMonitorBolt() throws Throwable
    {
        // Given
        File metricsFolder = tmpDir.newFolder( "metrics" );
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( boltConnector( "0" ).enabled, "true" )
                .setConfig( GraphDatabaseSettings.auth_enabled, "false" )
                .setConfig( MetricsSettings.boltMessagesEnabled, "true" )
                .setConfig( MetricsSettings.csvEnabled, "true" )
                .setConfig( MetricsSettings.csvPath, metricsFolder.getAbsolutePath() )
                .newGraphDatabase();

        // When
        conn = new SocketConnection()
                .connect( new HostnamePort( "localhost", 7687 ) )
                .send( acceptedVersions( 1, 0, 0, 0 ) )
                .send( chunk( Messages.init( "TestClient", map("scheme", "basic", "principal", "neo4j", "credentials", "neo4j") ) ) );

        // Then
        assertEventually( "init request shows up as recieved",
                () -> readLongValue( metricsCsv( metricsFolder, BoltMetrics.MESSAGES_RECIEVED ) ),
                equalTo( 1L ), 5, TimeUnit.SECONDS );
        assertEventually( "init request shows up as started",
                () -> readLongValue( metricsCsv( metricsFolder, BoltMetrics.MESSAGES_STARTED ) ),
                equalTo( 1L ), 5, TimeUnit.SECONDS );
        assertEventually( "init request shows up as done",
                () -> readLongValue( metricsCsv( metricsFolder, BoltMetrics.MESSAGES_DONE ) ),
                equalTo( 1L ), 5, TimeUnit.SECONDS );

        assertEventually( "queue time shows up",
                () -> readLongValue( metricsCsv( metricsFolder, BoltMetrics.TOTAL_QUEUE_TIME ) ),
                greaterThanOrEqualTo( 0L ), 5, TimeUnit.SECONDS );
        assertEventually( "processing time shows up",
                () -> readLongValue( metricsCsv( metricsFolder, BoltMetrics.TOTAL_PROCESSING_TIME ) ),
                greaterThanOrEqualTo( 0L ), 5, TimeUnit.SECONDS );

    }

    @After
    public void cleanup() throws Exception
    {
        conn.disconnect();
        db.shutdown();
    }

}
