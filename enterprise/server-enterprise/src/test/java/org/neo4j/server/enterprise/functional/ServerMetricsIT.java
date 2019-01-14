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
package org.neo4j.server.enterprise.functional;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.metrics.MetricsSettings;
import org.neo4j.metrics.source.server.ServerMetrics;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ServerMetricsIT
{
    @Rule
    public final TestDirectory folder = TestDirectory.testDirectory();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldShowServerMetrics() throws Throwable
    {
        // Given
        File metrics = folder.file( "metrics" );
        NeoServer server = EnterpriseServerBuilder.serverOnRandomPorts()
                .usingDataDir( folder.graphDbDir().getAbsolutePath() )
                .withProperty( MetricsSettings.metricsEnabled.name(), "true" )
                .withProperty( MetricsSettings.csvEnabled.name(), "true" )
                .withProperty( MetricsSettings.csvPath.name(), metrics.getPath() )
                .withProperty( MetricsSettings.csvInterval.name(), "100ms" )
                .persistent()
                .build();
        try
        {
            // when
            server.start();

            String host = "http://localhost:" + server.baseUri().getPort() +
                          ServerSettings.rest_api_path.getDefaultValue() + "/transaction/commit";

            for ( int i = 0; i < 5; i++ )
            {
                ClientResponse r = Client.create().resource( host ).accept( APPLICATION_JSON ).type( APPLICATION_JSON )
                        .post( ClientResponse.class, "{ 'statements': [ { 'statement': 'CREATE ()' } ] }" );
                assertEquals( 200, r.getStatus() );
            }

            // then
            assertMetricsExists( metrics, ServerMetrics.THREAD_JETTY_ALL );
            assertMetricsExists( metrics, ServerMetrics.THREAD_JETTY_IDLE );
        }
        finally
        {
            server.stop();
        }
    }

    private static void assertMetricsExists( File metricsPath, String metricsName ) throws InterruptedException
    {
        File file = metricsCsv( metricsPath, metricsName );
        assertEventually( () -> threadCountReader( file ), greaterThan( 0L ), 1, TimeUnit.MINUTES );
    }

    private static Long threadCountReader( File file ) throws InterruptedException
    {
        try
        {
            return readLongValue( file );
        }
        catch ( IOException io )
        {
            throw new UncheckedIOException( io );
        }
    }
}
