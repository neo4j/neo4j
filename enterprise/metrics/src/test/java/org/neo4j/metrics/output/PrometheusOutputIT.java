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
package org.neo4j.metrics.output;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.metrics.MetricsSettings.prometheusEnabled;
import static org.neo4j.metrics.MetricsSettings.prometheusEndpoint;
import static org.neo4j.metrics.source.db.EntityCountMetrics.COUNTS_NODE;
import static org.neo4j.metrics.source.db.EntityCountMetrics.COUNTS_RELATIONSHIP_TYPE;

@ExtendWith( TestDirectoryExtension.class )
public class PrometheusOutputIT
{
    @Resource
    public TestDirectory testDirectory;

    private GraphDatabaseService database;
    private String serverAddress;

    @BeforeEach
    public void setUp()
    {
        serverAddress = "localhost:" + PortAuthority.allocatePort();
        database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( prometheusEnabled, Settings.TRUE )
                .setConfig( prometheusEndpoint, serverAddress )
                .newGraphDatabase();
    }

    @AfterEach
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
