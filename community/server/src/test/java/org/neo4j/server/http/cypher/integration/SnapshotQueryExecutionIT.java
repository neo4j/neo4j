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
package org.neo4j.server.http.cypher.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.snapshot.TestTransactionVersionContextSupplier;
import org.neo4j.snapshot.TestVersionContext;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.server.helpers.CommunityServerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class SnapshotQueryExecutionIT extends ExclusiveServerTestBase
{
    private TestTransactionVersionContextSupplier testContextSupplier;
    private TestVersionContext testCursorContext;
    private CommunityNeoServer server;

    @Before
    public void setUp() throws Exception
    {
        testContextSupplier = new TestTransactionVersionContextSupplier();
        var dependencies = new Dependencies();
        dependencies.satisfyDependencies( testContextSupplier );
        server = serverOnRandomPorts()
                .withProperty( GraphDatabaseSettings.snapshot_query.name(), TRUE )
                .withDependencies( dependencies )
                .build();
        server.start();
        prepareCursorContext();
        createData( server.getDatabaseService().getDatabase() );
    }

    private void prepareCursorContext()
    {
        testCursorContext = TestVersionContext
                .testCursorContext( server.getDatabaseService().getDatabaseManagementService(),
                                    server.getDatabaseService().getDatabase().databaseName() );
        testContextSupplier.setCursorContext( testCursorContext );
    }

    private static void createData( GraphDatabaseService database )
    {
        Label label = Label.label( "toRetry" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "c", "d" );
            transaction.commit();
        }
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
    public void executeQueryWithSingleRetry()
    {
        HTTP.Response response = executeOverHTTP( "MATCH (n) RETURN n.c" );
        assertThat( response.status(), equalTo( 200 ) );
        Map<String,List<Map<String,List<Map<String,List<String>>>>>> content = response.content();
        assertEquals( "d", content.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "row" ).get( 0 ) );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
    }

    @Test
    public void queryThatModifiesDataAndSeesUnstableSnapshotShouldThrowException()
    {
        HTTP.Response response = executeOverHTTP( "MATCH (n:toRetry) CREATE () RETURN n.c" );
        Map<String,List<Map<String,String>>> content = response.content();
        assertEquals( "Unable to get clean data snapshot for query 'MATCH (n:toRetry) CREATE () RETURN n.c' that performs updates.",
                      content.get( "errors" ).get( 0 ).get( "message" ) );
    }

    private HTTP.Response executeOverHTTP( String query )
    {
        HTTP.Builder httpClientBuilder = HTTP.withBaseUri( server.baseUri() );
        HTTP.Response transactionStart = httpClientBuilder.POST( txEndpoint() );
        assertThat( transactionStart.status(), equalTo( 201 ) );
        return httpClientBuilder.POST( transactionStart.location(), quotedJson( "{ 'statements': [ { 'statement': '" + query + "' } ] }" ) );
    }
}
