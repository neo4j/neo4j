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
package org.neo4j.harness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;

import org.neo4j.configuration.ssl.PemSslPolicyConfig;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.Neo4jBuilder;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static java.lang.System.lineSeparator;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.HTTPS;
import static org.neo4j.harness.internal.TestNeo4jBuilders.newInProcessBuilder;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class FixturesTestIT
{
    @Inject
    private TestDirectory testDir;

    @Test
    void shouldAccepSingleCypherFileAsFixture() throws Exception
    {
        // Given
        File targetFolder = testDir.directory();
        File fixture = new File( targetFolder, "fixture.cyp" );
        FileUtils.writeToFile(fixture,
                "CREATE (u:User)" +
                "CREATE (a:OtherUser)", false);

        // When
        try ( InProcessNeo4j server = getServerBuilder( targetFolder ).withFixture( fixture ).build() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().toString() + "db/neo4j/tx/commit",
                    quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

            assertThat( response.status(), equalTo( 200 ) );
            assertThat(response.get( "results" ).get(0).get("data").size(), equalTo(1));
        }
    }

    @Test
    void shouldAcceptFolderWithCypFilesAsFixtures() throws Exception
    {
        // Given two files in the root folder
        File targetFolder = testDir.directory();
        FileUtils.writeToFile( new File( targetFolder, "fixture1.cyp" ), "CREATE (u:User)\n" + "CREATE (a:OtherUser)",
                false );
        FileUtils.writeToFile( new File( targetFolder, "fixture2.cyp" ), "CREATE (u:User)\n" + "CREATE (a:OtherUser)",
                false );

        // And given one file in a sub directory
        File subDir = new File( targetFolder, "subdirectory" );
        subDir.mkdir();
        FileUtils.writeToFile( new File( subDir, "subDirFixture.cyp" ), "CREATE (u:User)\n" + "CREATE (a:OtherUser)",
                false );

        // When
        try ( InProcessNeo4j server = getServerBuilder( targetFolder ).withFixture( targetFolder ).build() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().toString() + "db/neo4j/tx/commit",
                    quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

            assertThat( response.toString(), response.get( "results" ).get(0).get("data").size(), equalTo(3) );
        }
    }

    @Test
    void shouldHandleMultipleFixtures() throws Exception
    {
        // Given two files in the root folder
        File targetFolder = testDir.directory();
        File fixture1 = new File( targetFolder, "fixture1.cyp" );
        FileUtils.writeToFile( fixture1,
                "CREATE (u:User)\n" +
                "CREATE (a:OtherUser)", false);
        File fixture2 = new File( targetFolder, "fixture2.cyp" );
        FileUtils.writeToFile( fixture2,
                "CREATE (u:User)\n" +
                "CREATE (a:OtherUser)", false);

        // When
        try ( InProcessNeo4j server = getServerBuilder( targetFolder )
                .withFixture( fixture1 )
                .withFixture( fixture2 )
                .build() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().toString() + "db/neo4j/tx/commit",
                    quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

            assertThat( response.get( "results" ).get(0).get("data").size(), equalTo(2));
        }
    }

    @Test
    void shouldHandleStringFixtures() throws Exception
    {
        // Given two files in the root folder
        File targetFolder = testDir.directory();

        // When
        try ( InProcessNeo4j server = getServerBuilder( targetFolder )
                .withFixture( "CREATE (a:User)" )
                .build() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().toString() + "db/neo4j/tx/commit",
                    quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

            assertThat( response.get( "results" ).get(0).get("data").size(), equalTo(1));
        }
    }

    @Test
    void shouldIgnoreEmptyFixtureFiles() throws Exception
    {
        // Given two files in the root folder
        File targetFolder = testDir.directory();
        FileUtils.writeToFile( new File( targetFolder, "fixture1.cyp" ), "CREATE (u:User)\n" + "CREATE (a:OtherUser)",
                false );
        FileUtils.writeToFile( new File( targetFolder, "fixture2.cyp" ), "", false );

        // When
        try ( InProcessNeo4j server = getServerBuilder( targetFolder )
                .withFixture( targetFolder ).build() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().toString() + "db/neo4j/tx/commit",
                    quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

            assertThat( response.get( "results" ).get(0).get("data").size(), equalTo(1));
        }
    }

    @Test
    void shouldHandleFixturesWithSyntaxErrorsGracefully() throws Exception
    {
        // Given two files in the root folder
        File targetFolder = testDir.directory();
        FileUtils.writeToFile( new File( targetFolder, "fixture1.cyp" ), "this is not a valid cypher statement", false );

        // When
        try ( InProcessNeo4j ignore = getServerBuilder( targetFolder )
                .withFixture( targetFolder ).build() )
        {
            fail( "Should have thrown exception" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e.getMessage(), equalTo(
                    "Invalid input 't': expected <init> (line 1, column 1 (offset: 0))" + lineSeparator() +
                    "\"this is not a valid cypher statement\"" + lineSeparator() + " ^" ) );
        }
    }

    @Test
    void shouldHandleFunctionFixtures() throws Exception
    {
        // Given two files in the root folder
        File targetFolder = testDir.directory();

        // When
        try ( InProcessNeo4j server = getServerBuilder( targetFolder )
                .withFixture( graphDatabaseService ->
                {
                    try ( Transaction tx = graphDatabaseService.beginTx() )
                    {
                        graphDatabaseService.createNode( Label.label( "User" ) );
                        tx.commit();
                    }
                    return null;
                } )
                .build() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().toString() + "db/neo4j/tx/commit",
                    quotedJson( "{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}" ) );

            assertThat( response.get( "results" ).get( 0 ).get( "data" ).size(), equalTo( 1 ) );
        }
    }

    private Neo4jBuilder getServerBuilder( File targetFolder )
    {
        SelfSignedCertificateFactory.create( testDir.directory() );
        return newInProcessBuilder( targetFolder )
                .withConfig( PemSslPolicyConfig.forScope( BOLT ).base_directory, testDir.directory().toPath() )
                .withConfig( PemSslPolicyConfig.forScope( HTTPS ).base_directory, testDir.directory().toPath() );
    }

}
