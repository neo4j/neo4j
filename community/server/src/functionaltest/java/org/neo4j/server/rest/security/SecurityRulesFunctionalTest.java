/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rest.security;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RESTDocsGenerator;
import org.neo4j.test.TestData;

public class SecurityRulesFunctionalTest
{
    private NeoServerWithEmbeddedWebServer server;

    private FunctionalTestHelper functionalTestHelper;

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void should401WithBasicChallengeWhenASecurityRuleFails() throws Exception
    {
        server = ServerBuilder.server()
                .withDefaultDatabaseTuning()
                .withSecurityRules( PermanentlyFailingSecurityRule.class.getCanonicalName() )
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );

        JaxRsResponse response = gen.get()
                .expectedStatus( 401 )
                .expectedHeader( "WWW-Authenticate" )
                .post( functionalTestHelper.nodeUri() )
                .response();

        assertThat( response.getHeaders()
                .getFirst( "WWW-Authenticate" ), containsString( PermanentlyFailingSecurityRule.REALM ) );
    }

    // === DISABLED BY MP 2011-10-03 DUE TO COMPILATION ERROR AND AUTHOR OFFLINE ===
//    @Test
//    public void should401WithBasicChallengeIfAnyOneOfTheRulesFails() throws Exception
//    {
//        server = ServerBuilder.server()
//                .withDefaultDatabaseTuning()
//                .withSecurityRules( PermanentlyPassingSecurityRule.class.getCanonicalName(),
//                        PermanentlyFailingSecurityRule.class.getCanonicalName() )
//                .build();
//        server.start();
//        functionalTestHelper = new FunctionalTestHelper( server );
//
//        JaxRsResponse response = gen.get()
//                .expectedStatus( 401 )
//                .expectedHeader( "WWW-Authenticate" )
//                .post( functionalTestHelper.nodeUri() )
//                .response();
//
//        assertThat( response.getHeaders()
//                .getFirst( "WWW-Authenticate" ), containsString( PermanentlyFailingSecurityRule.REALM ) );
//    }
//
//    @Test
//    public void shouldRespondWith201IfAllTheRulesPassWhenCreatingANode() throws Exception
//    {
//        server = ServerBuilder.server()
//                .withDefaultDatabaseTuning()
//                .withSecurityRules( PermanentlyPassingSecurityRule.class.getCanonicalName() )
//                .build();
//        server.start();
//        functionalTestHelper = new FunctionalTestHelper( server );
//
//        gen.get()
//                .expectedStatus( 201 )
//                .expectedHeader( "Location" )
//                .post( functionalTestHelper.nodeUri() )
//                .response();
//    }
}
