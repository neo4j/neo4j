/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.metrics;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.messaging.message.InitMessage;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private GraphDatabaseAPI db;
    private TransportConnection conn;

    private final TransportTestUtil util = new TransportTestUtil( new Neo4jPackV1() );

    @Test
    public void shouldMonitorBolt() throws Throwable
    {
        int port = PortAuthority.allocatePort();

        // Given
        File metricsFolder = testDirectory.directory( "metrics" );
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( new BoltConnector( "bolt" ).type, "BOLT" )
                .setConfig( new BoltConnector( "bolt" ).enabled, "true" )
                .setConfig( new BoltConnector( "bolt" ).listen_address, "localhost:" + port )
                .setConfig( GraphDatabaseSettings.auth_enabled, "false" )
                .setConfig( MetricsSettings.metricsEnabled, "false" )
                .setConfig( MetricsSettings.boltMessagesEnabled, "true" )
                .setConfig( MetricsSettings.csvEnabled, "true" )
                .setConfig( MetricsSettings.csvInterval, "100ms" )
                .setConfig( MetricsSettings.csvPath, metricsFolder.getAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();

        // When
        conn = new SocketConnection()
                .connect( new HostnamePort( "localhost", port ) )
                .send( util.acceptedVersions( 1, 0, 0, 0 ) )
                .send( util.chunk( InitMessage.init( "TestClient",
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
