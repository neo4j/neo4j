/**
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
package org.neo4j.harness;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Function;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.Mute;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class JUnitRuleTest
{
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture( "CREATE (u:User)" )
            .withFixture( new Function<GraphDatabaseService, Void>()
            {
                @Override
                public Void apply( GraphDatabaseService graphDatabaseService ) throws RuntimeException
                {
                    try ( Transaction tx = graphDatabaseService.beginTx() )
                    {
                        graphDatabaseService.createNode( DynamicLabel.label( "User" ));
                        tx.success();
                    }
                    return null;
                }
            } )
            .withExtension( "/test", MyUnmanagedExtension.class );

    @Rule public Mute mute = Mute.muteAll();

    @Test
    public void shouldExtensionWork() throws Exception
    {
        // Given the rule in the beginning of this class

        // When I run this test

        // Then
        assertThat( HTTP.GET( neo4j.httpURI().resolve("test/myExtension").toString() ).status(), equalTo( 234 ) );
    }

    @Test
    public void shouldFixturesWork() throws Exception
    {
        // Given the rule in the beginning of this class

        // When I run this test

        // Then
        HTTP.Response response = HTTP.POST( neo4j.httpURI().toString() + "db/data/transaction/commit",
                quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

        assertThat( response.get( "results" ).get(0).get("data").size(), equalTo(2));
    }

    @Test
    public void shouldGraphDatabaseServiceBeAccessible()
    {
        // Given the rule in the beginning of this class

        // When I run this test

        // Then
        assertEquals(2, IteratorUtil.count(
                neo4j.getGraphDatabaseService().execute( "MATCH (n:User) RETURN n" )
        ));
    }
}
