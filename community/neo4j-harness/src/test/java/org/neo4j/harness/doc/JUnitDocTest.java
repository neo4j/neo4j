/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.harness.doc;

import java.net.URI;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Function;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.*;

public class JUnitDocTest
{
    @Rule public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    // START SNIPPET: useJUnitRule
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture( "CREATE (admin:Admin)" )
            .withFixture( new Function<GraphDatabaseService, Void>()
            {
                @Override
                public Void apply( GraphDatabaseService graphDatabaseService ) throws RuntimeException
                {
                    try (Transaction tx = graphDatabaseService.beginTx())
                    {
                        graphDatabaseService.createNode( DynamicLabel.label( "Admin" ));
                        tx.success();
                    }
                    return null;
                }
            } );

    @Test
    public void shouldWorkWithServer() throws Exception
    {
        // Given
        URI serverURI = neo4j.httpURI();

        // When I access the server
        HTTP.Response response = HTTP.GET( serverURI.toString() );

        // Then it should reply
        assertEquals(200, response.status());

        // and we have access to underlying GraphDatabaseService
        try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
            assertEquals( 2, IteratorUtil.count(
                    neo4j.getGraphDatabaseService().findNodes( DynamicLabel.label( "Admin" ) )
            ));
            tx.success();
        }
    }
    // END SNIPPET: useJUnitRule

}
