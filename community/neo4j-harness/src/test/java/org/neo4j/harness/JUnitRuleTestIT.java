/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.junit.runners.model.Statement;

import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.server.ServerTestUtils.getSharedTestTemporaryFolder;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class JUnitRuleTestIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture( "CREATE (u:User)" )
            .withConfig( LegacySslPolicyConfig.certificates_directory.name(),
                    getRelativePath( getSharedTestTemporaryFolder(), LegacySslPolicyConfig.certificates_directory ) )
            .withFixture( graphDatabaseService ->
            {
                try ( Transaction tx = graphDatabaseService.beginTx() )
                {
                    graphDatabaseService.createNode( Label.label( "User" ) );
                    tx.success();
                }
                return null;
            } )
            .withExtension( "/test", MyUnmanagedExtension.class );

    @Test
    public void shouldExtensionWork() throws Exception
    {
        // Given the rule in the beginning of this class

        // When I run this test

        // Then
        assertThat( HTTP.GET( neo4j.httpURI().resolve( "test/myExtension" ).toString() ).status(), equalTo( 234 ) );
    }

    @Test
    public void shouldFixturesWork() throws Exception
    {
        // Given the rule in the beginning of this class

        // When I run this test

        // Then
        HTTP.Response response = HTTP.POST( neo4j.httpURI().toString() + "db/data/transaction/commit",
                quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

        assertThat( response.get( "results" ).get( 0 ).get( "data" ).size(), equalTo( 2 ) );
    }

    @Test
    public void shouldGraphDatabaseServiceBeAccessible()
    {
        // Given the rule in the beginning of this class

        // When I run this test

        // Then
        assertEquals( 2, Iterators.count(
                neo4j.getGraphDatabaseService().execute( "MATCH (n:User) RETURN n" )
        ) );
    }

    @Test
    public void shouldRuleWorkWithExsitingDirectory()
    {
        // given

        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.directory() )
                .newGraphDatabase();
        try
        {
            db.execute( "CREATE ()" );
        }
        finally
        {
            db.shutdown();
        }

        // When a rule with an pre-populated graph db directory is used
        final Neo4jRule ruleWithDirectory =
                new Neo4jRule( testDirectory.directory() ).copyFrom( testDirectory.directory() );
        ruleWithDirectory.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                // Then the database is not empty
                Result result = ruleWithDirectory.getGraphDatabaseService()
                        .execute( "MATCH (n) RETURN count(n) AS " + "count" );

                List<Object> column = Iterators.asList( result.columnAs( "count" ) );
                assertEquals( 1, column.size() );
                assertEquals( 1, column.get( 0 ) );
            }
        }, null );
    }
}
