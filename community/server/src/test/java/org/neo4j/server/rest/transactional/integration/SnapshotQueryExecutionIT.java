/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.rest.transactional.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.Database;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.server.helpers.CommunityServerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class SnapshotQueryExecutionIT extends ExclusiveServerTestBase
{

    private CommunityNeoServer server;

    @Before
    public void setUp() throws Exception
    {
        server = serverOnRandomPorts().withProperty( GraphDatabaseSettings.snapshot_query.name(), Settings.TRUE ).build();
        server.start();
    }

    @After
    public void tearDown()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void executeQueryWithSnapshotEngine()
    {
        Database database = server.getDatabase();
        GraphDatabaseFacade graph = database.getGraph();
        try ( Transaction transaction = graph.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = graph.createNode();
                node.setProperty( "a", "b" );
            }
            transaction.success();
        }

        HTTP.Builder httpClientBuilder = HTTP.withBaseUri( server.baseUri() );
        HTTP.Response transactionStart = httpClientBuilder.POST( transactionURI() );
        assertThat( transactionStart.status(), equalTo( 201 ) );
        HTTP.Response response =
                httpClientBuilder.POST( transactionStart.location(), quotedJson( "{ 'statements': [ { 'statement': 'MATCH (n) RETURN n' } ] }" ) );
        assertThat( response.status(), equalTo( 200 ) );
    }

    private String transactionURI()
    {
        return "db/data/transaction";
    }
}
