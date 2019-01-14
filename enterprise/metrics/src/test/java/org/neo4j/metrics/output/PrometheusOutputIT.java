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
package org.neo4j.metrics.output;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;
import static org.neo4j.metrics.MetricsSettings.prometheusEnabled;
import static org.neo4j.metrics.MetricsSettings.prometheusEndpoint;
import static org.neo4j.metrics.source.db.EntityCountMetrics.COUNTS_NODE;
import static org.neo4j.metrics.source.db.EntityCountMetrics.COUNTS_RELATIONSHIP_TYPE;

public class PrometheusOutputIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private GraphDatabaseService database;
    private String serverAddress;

    @Before
    public void setUp()
    {
        serverAddress = "localhost:" + PortAuthority.allocatePort();
        database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( prometheusEnabled, Settings.TRUE )
                .setConfig( prometheusEndpoint, serverAddress )
                .newGraphDatabase();
    }

    @After
    public void tearDown()
    {
        database.shutdown();
    }

    @Test
    public void httpEndpointShouldBeAvailableAndResponsive() throws IOException
    {
        String url = "http://" + serverAddress + "/metrics";
        URLConnection connection = new URL( url ).openConnection();
        connection.setDoOutput( true );
        connection.connect();
        Scanner s = new Scanner( connection.getInputStream(), "UTF-8" ).useDelimiter( "\\A" );

        assertTrue( s.hasNext() );
        String response = s.next();
        assertTrue( response.contains( COUNTS_NODE ) );
        assertTrue( response.contains( COUNTS_RELATIONSHIP_TYPE ) );
    }
}
