package org.neo4j.server.rest.security;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
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
    private static NeoServerWithEmbeddedWebServer server;

    private static FunctionalTestHelper functionalTestHelper;

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    @BeforeClass
    public static void startServer() throws IOException
    {
        server = ServerBuilder.server()
                .withDefaultDatabaseTuning()
                .withSecurityRules( PermanentlyFailingSecurityRule.class.getCanonicalName() )
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    @Test
    public void should401WithBasicChallengeWhenASecurityRuleFails()
    {
        JaxRsResponse response = gen.get()
                .expectedStatus( 401 )
                .expectedHeader( "WWW-Authenticate" )
                .post( functionalTestHelper.nodeUri() )
                .response();
        
        assertThat( response.getHeaders().getFirst( "WWW-Authenticate" ), containsString(PermanentlyFailingSecurityRule.REALM) );
    }
}
