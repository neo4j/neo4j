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

    @Test
    public void should401WithBasicChallengeIfAnyOneOfTheRulesFails() throws Exception
    {
        server = ServerBuilder.server()
                .withDefaultDatabaseTuning()
                .withSecurityRules( PermanentlyPassingSecurityRule.class.getCanonicalName(),
                        PermanentlyFailingSecurityRule.class.getCanonicalName() )
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

    @Test
    public void shouldRespondWith201IfAllTheRulesPassWhenCreatingANode() throws Exception
    {
        server = ServerBuilder.server()
                .withDefaultDatabaseTuning()
                .withSecurityRules( PermanentlyPassingSecurityRule.class.getCanonicalName() )
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );

        gen.get()
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeUri() )
                .response();
    }
}
