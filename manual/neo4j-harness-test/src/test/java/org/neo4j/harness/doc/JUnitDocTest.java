/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.harness.doc;

import java.net.URI;

import org.junit.Rule;
import org.junit.Test;

import java.util.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.server.HTTP;

import org.neo4j.server.configuration.ServerSettings;

import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.server.ServerTestUtils.getSharedTestTemporaryFolder;

import static org.junit.Assert.*;

public class JUnitDocTest
{
    @Rule public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    // START SNIPPET: useJUnitRule
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture( "CREATE (admin:Admin)" )
            .withConfig( ServerSettings.tls_key_file.name(),
                    getRelativePath( getSharedTestTemporaryFolder(), ServerSettings.tls_key_file ) )
            .withConfig( ServerSettings.tls_certificate_file.name(),
                    getRelativePath( getSharedTestTemporaryFolder(), ServerSettings.tls_certificate_file ) )
            .withFixture( new Function<GraphDatabaseService, Void>()
            {
                @Override
                public Void apply( GraphDatabaseService graphDatabaseService ) throws RuntimeException
                {
                    try (Transaction tx = graphDatabaseService.beginTx())
                    {
                        graphDatabaseService.createNode( Label.label( "Admin" ) );
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
                    neo4j.getGraphDatabaseService().findNodes( Label.label( "Admin" ) )
            ));
            tx.success();
        }
    }
    // END SNIPPET: useJUnitRule

}
