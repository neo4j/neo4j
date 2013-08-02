/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server;

import com.sun.jersey.api.client.ClientResponse;
import org.junit.After;
import org.junit.Test;

import org.neo4j.server.statistic.StatisticRecord;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static com.sun.jersey.api.client.Client.create;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static org.neo4j.server.configuration.Configurator.WEBSERVER_ENABLE_STATISTICS_COLLECTION;
import static org.neo4j.server.helpers.CommunityServerBuilder.server;

public class StatisticsCollectionEnabledDocIT extends ExclusiveServerTestBase
{

    private CommunityNeoServer server;

    @After
    public void stopTheServer()
    {
        server.stop();
    }

    @Test
    public void statisticsShouldBeDisabledByDefault() throws Exception
    {
        server = server()
                .usingDatabaseDir( folder.getRoot().getAbsolutePath() )
                .build();
        server.start();

        create().resource( server.baseUri() ).get( ClientResponse.class );

        StatisticRecord snapshot = server.statisticsCollector.createSnapshot();
        assertThat( snapshot.getRequests(), is( 0L ) );
    }

    @Test
    public void statisticsCouldBeEnabled() throws Exception
    {
        server = server().withProperty( WEBSERVER_ENABLE_STATISTICS_COLLECTION, "true" )
                .usingDatabaseDir( folder.getRoot().getAbsolutePath() )
                .build();
        server.start();

        create().resource( server.baseUri() ).get( ClientResponse.class );

        StatisticRecord snapshot = server.statisticsCollector.createSnapshot();
        assertThat( snapshot.getRequests(), greaterThan( 0L ) );
    }
}
