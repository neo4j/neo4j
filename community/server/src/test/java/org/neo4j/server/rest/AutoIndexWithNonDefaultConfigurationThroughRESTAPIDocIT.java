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
package org.neo4j.server.rest;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.test.TestData;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class AutoIndexWithNonDefaultConfigurationThroughRESTAPIDocIT extends ExclusiveServerTestBase
{
    private static CommunityNeoServer server;
    private static FunctionalTestHelper functionalTestHelper;

    @ClassRule
    public static TemporaryFolder staticFolder = new TemporaryFolder();

    public
    @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    @Before
    public void setUp()
    {
        gen.get().setSection( "dev/rest-api" );
    }

    @BeforeClass
    public static void allocateServer() throws IOException
    {
        server = CommunityServerBuilder.server()
                .usingDatabaseDir( staticFolder.getRoot().getAbsolutePath() )
                .withAutoIndexingEnabledForNodes( "foo", "bar" )
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    /**
     * Create an auto index for nodes with specific configuration.
     */
    @Test
    public void shouldCreateANodeAutoIndexWithGivenFullTextConfiguration() throws Exception
    {
        String responseBody = gen.get()
                .expectedStatus( 201 )
                .payload( "{\"name\":\"node_auto_index\", \"config\":{\"type\":\"fulltext\",\"provider\":\"lucene\"}}" )
                .post( functionalTestHelper.nodeIndexUri() )
                .entity();

        assertThat( responseBody, containsString( "\"type\" : \"fulltext\"" ) );
    }

    /**
     * Create an auto index for relationships with specific configuration.
     */
    @Test
    public void shouldCreateARelationshipAutoIndexWithGivenFullTextConfiguration() throws Exception
    {
        String responseBody = gen.get()
                .expectedStatus( 201 )
                .payload(
                        "{\"name\":\"relationship_auto_index\", \"config\":{\"type\":\"fulltext\"," +
                                "\"provider\":\"lucene\"}}" )
                .post( functionalTestHelper.relationshipIndexUri() )
                .entity();

        assertThat( responseBody, containsString( "\"type\" : \"fulltext\"" ) );
    }

}
