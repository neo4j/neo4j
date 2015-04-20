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
package org.neo4j.examples.server.unmanaged;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

public class UnmanagedExtensionsDocIT
{
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture( "UNWIND ['Keanu Reeves','Hugo Weaving','Carrie-Anne Moss','Laurence Fishburne'] AS actor " +
                    "MERGE (m:Movie  {name: 'The Matrix'}) " +
                    "MERGE (p:Person {name: actor}) " +
                    "MERGE (p)-[:ACTED_IN]->(m) " )
            .withExtension( "/path/to/my/extension1", ColleaguesCypherExecutionResource.class )
            .withExtension( "/path/to/my/extension2", ColleaguesResource.class );

    @Test
    public void shouldRetrieveColleaguesViaExecutionEngine() throws IOException
    {
        // When
        HTTP.Response response = HTTP.GET( neo4j.httpURI().resolve(
                "/path/to/my/extension1/colleagues-cypher-execution/Keanu%20Reeves" ).toString() );

        // Then
        assertEquals( 200, response.status() );

        Map<String, Object> content = response.content();
        List<String> colleagues = (List<String>) content.get( "colleagues" );

        assertThat( colleagues.size(), equalTo( 3 ) );
        assertThat( colleagues, hasItem( "Laurence Fishburne" ) );
        assertThat( colleagues, hasItem( "Hugo Weaving" ) );
        assertThat( colleagues, hasItem( "Carrie-Anne Moss" ) );
    }

    @Test
    public void shouldRetrieveColleaguesViaTransactionAPI() throws IOException
    {
        // When
        HTTP.Response response = HTTP.GET( neo4j.httpURI().resolve(
                "/path/to/my/extension2/colleagues/Keanu%20Reeves" ).toString() );

        // Then
        assertEquals( 200, response.status() );

        Map<String, Object> content = response.content();
        List<String> colleagues = (List<String>) content.get( "colleagues" );

        assertThat( colleagues.size(), equalTo( 3 ) );
        assertThat( colleagues, hasItem( "Laurence Fishburne" ) );
        assertThat( colleagues, hasItem( "Hugo Weaving" ) );
        assertThat( colleagues, hasItem( "Carrie-Anne Moss" ) );
    }
}
