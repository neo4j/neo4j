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

import java.io.File;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.bolt.v1.messaging.message.InitMessage;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.acceptedVersions;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.chunk;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.metrics.source.db.BoltMetrics.MESSAGES_DONE;
import static org.neo4j.metrics.source.db.BoltMetrics.MESSAGES_RECIEVED;
import static org.neo4j.metrics.source.db.BoltMetrics.MESSAGES_STARTED;
import static org.neo4j.metrics.source.db.BoltMetrics.SESSIONS_STARTED;
import static org.neo4j.metrics.source.db.BoltMetrics.TOTAL_PROCESSING_TIME;
import static org.neo4j.metrics.source.db.BoltMetrics.TOTAL_QUEUE_TIME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class BoltMetricsIT
{
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    private GraphDatabaseAPI db;
    private TransportConnection conn;

    @Test
    public void shouldMonitorBolt() throws Throwable
    {
        // Given
        File metricsFolder = tmpDir.newFolder( "metrics" );
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig( new BoltConnector( "bolt" ).type, "BOLT" )
                .setConfig( new BoltConnector( "bolt" ).enabled, "true" )
                .setConfig( GraphDatabaseSettings.auth_enabled, "false" )
                .setConfig( MetricsSettings.boltMessagesEnabled, "true" )
                .setConfig( MetricsSettings.csvEnabled, "true" )
                .setConfig( MetricsSettings.csvInterval, "100ms" )
                .setConfig( MetricsSettings.csvPath, metricsFolder.getAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();

        // When
        conn = new SocketConnection()
                .connect( new HostnamePort( "localhost", 7687 ) )
                .send( acceptedVersions( 1, 0, 0, 0 ) )
                .send( chunk( InitMessage.init( "TestClient",
                        map("scheme", "basic", "principal", "neo4j", "credentials", "neo4j") ) ) );

        // Then
        assertEventually( "session shows up as started",
                () -> readLongValue( metricsCsv( metricsFolder, SESSIONS_STARTED ) ), equalTo( 1L ), 5, SECONDS );
        assertEventually( "init request shows up as received",
                () -> readLongValue( metricsCsv( metricsFolder, MESSAGES_RECIEVED ) ), equalTo( 1L ), 5, SECONDS );
        assertEventually( "init request shows up as started",
                () -> readLongValue( metricsCsv( metricsFolder, MESSAGES_STARTED ) ), equalTo( 1L ), 5, SECONDS );
        assertEventually( "init request shows up as done",
                () -> readLongValue( metricsCsv( metricsFolder, MESSAGES_DONE ) ), equalTo( 1L ), 5, SECONDS );

        assertEventually( "queue time shows up",
                () -> readLongValue( metricsCsv( metricsFolder, TOTAL_QUEUE_TIME ) ),
                greaterThanOrEqualTo( 0L ), 5, SECONDS );
        assertEventually( "processing time shows up",
                () -> readLongValue( metricsCsv( metricsFolder, TOTAL_PROCESSING_TIME ) ),
                greaterThanOrEqualTo( 0L ), 5, SECONDS );
    }

    @After
    public void cleanup() throws Exception
    {
        conn.disconnect();
        db.shutdown();
    }

}
