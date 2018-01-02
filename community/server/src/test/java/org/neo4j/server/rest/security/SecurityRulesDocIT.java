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
package org.neo4j.server.rest.security;

import org.dummy.web.service.DummyThirdPartyWebService;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import javax.ws.rs.core.MediaType;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RESTDocsGenerator;
import org.neo4j.test.TestData;
import org.neo4j.test.TestData.Title;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SecurityRulesDocIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    private FunctionalTestHelper functionalTestHelper;

    @Rule
    public TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    @Title( "Enforcing Server Authorization Rules" )
    @Documented( "In this example, a (dummy) failing security rule is registered to deny\n" +
                 "access to all URIs to the server by listing the rules class in\n" +
                 "'neo4j-server.properties':\n" +
                 "\n" +
                 "@@config\n" +
                 "\n" +
                 "with the rule source code of:\n" +
                 "\n" +
                 "@@failingRule\n" +
                 "\n" +
                 "With this rule registered, any access to the server will be\n" +
                 "denied. In a production-quality implementation the rule\n" +
                 "will likely lookup credentials/claims in a 3rd-party\n" +
                 "directory service (e.g. LDAP) or in a local database of\n" +
                 "authorized users." )
    public void should401WithBasicChallengeWhenASecurityRuleFails()
            throws Exception
    {
        server = CommunityServerBuilder.server().withDefaultDatabaseTuning().withSecurityRules(
                PermanentlyFailingSecurityRule.class.getCanonicalName() )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        gen.get().addSnippet(
                "config",
                "\n[source,properties]\n----\norg.neo4j.server.rest.security_rules=my.rules" +
                        ".PermanentlyFailingSecurityRule\n----\n" );
        gen.get().addTestSourceSnippets( PermanentlyFailingSecurityRule.class,
                "failingRule" );
        functionalTestHelper = new FunctionalTestHelper( server );
        gen.get().setSection( "ops" );
        JaxRsResponse response = gen.get().expectedStatus( 401 ).expectedHeader(
                "WWW-Authenticate" ).post( functionalTestHelper.nodeUri() ).response();

        assertThat( response.getHeaders().getFirst( "WWW-Authenticate" ),
                containsString( "Basic realm=\""
                        + PermanentlyFailingSecurityRule.REALM + "\"" ) );
    }

    @Test
    public void should401WithBasicChallengeIfAnyOneOfTheRulesFails()
            throws Exception
    {
        server = CommunityServerBuilder.server().withDefaultDatabaseTuning().withSecurityRules(
                PermanentlyFailingSecurityRule.class.getCanonicalName(),
                PermanentlyPassingSecurityRule.class.getCanonicalName() )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );

        JaxRsResponse response = gen.get().expectedStatus( 401 ).expectedHeader(
                "WWW-Authenticate" ).post( functionalTestHelper.nodeUri() ).response();

        assertThat( response.getHeaders().getFirst( "WWW-Authenticate" ),
                containsString( "Basic realm=\""
                        + PermanentlyFailingSecurityRule.REALM + "\"" ) );
    }

    @Test
    public void shouldInvokeAllSecurityRules() throws Exception
    {
        // given
        server = CommunityServerBuilder.server().withDefaultDatabaseTuning().withSecurityRules(
                NoAccessToDatabaseSecurityRule.class.getCanonicalName(),
                NoAccessToWebAdminSecurityRule.class.getCanonicalName() )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );

        // when
        gen.get().expectedStatus( 401 ).get( functionalTestHelper.dataUri() ).response();
        gen.get().expectedStatus( 401 ).expectedType( MediaType.TEXT_HTML_TYPE )
                .get( functionalTestHelper.webAdminUri() ).response();

        // then
        assertTrue( NoAccessToDatabaseSecurityRule.wasInvoked() );
        assertTrue(NoAccessToWebAdminSecurityRule.wasInvoked());
    }

    @Test
    public void shouldRespondWith201IfAllTheRulesPassWhenCreatingANode()
            throws Exception
    {
        server = CommunityServerBuilder.server().withDefaultDatabaseTuning().withSecurityRules(
                PermanentlyPassingSecurityRule.class.getCanonicalName() )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );

        gen.get().expectedStatus( 201 ).expectedHeader( "Location" ).post(
                functionalTestHelper.nodeUri() ).response();
    }

    @Test
    @Title( "Using Wildcards to Target Security Rules" )
    @Documented( "In this example, a security rule is registered to deny\n" +
                 "access to all URIs to the server by listing the rule(s) class(es) in\n" +
                 "'neo4j-server.properties'.\n" +
                 "In this case, the rule is registered\n" +
                 "using a wildcard URI path (where `*` characters can be used to signify\n" +
                 "any part of the path). For example `/users*` means the rule\n" +
                 "will be bound to any resources under the `/users` root path. Similarly\n" +
                 "`/users*type*` will bind the rule to resources matching\n" +
                 "URIs like `/users/fred/type/premium`.\n" +
                 "\n" +
                 "@@config\n" +
                 "\n" +
                 "with the rule source code of:\n" +
                 "\n" +
                 "@@failingRuleWithWildcardPath\n" +
                 "\n" +
                 "With this rule registered, any access to URIs under /protected/ will be\n" +
                 "denied by the server. Using wildcards allows flexible targeting of security rules to\n" +
                 "arbitrary parts of the server's API, including any unmanaged extensions or managed\n" +
                 "plugins that have been registered." )
    public void aSimpleWildcardUriPathShould401OnAccessToProtectedSubPath()
            throws Exception
    {
        String mountPoint = "/protected/tree/starts/here" + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT;
        server = CommunityServerBuilder.server().withDefaultDatabaseTuning()
                .withThirdPartyJaxRsPackage( "org.dummy.web.service",
                        mountPoint )
                .withSecurityRules(
                        PermanentlyFailingSecurityRuleWithWildcardPath.class.getCanonicalName() )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();

        gen.get()
                .addSnippet(
                        "config",
                        "\n[source,properties]\n----\norg.neo4j.server.rest.security_rules=my.rules" +
                                ".PermanentlyFailingSecurityRuleWithWildcardPath\n----\n" );

        gen.get().addTestSourceSnippets( PermanentlyFailingSecurityRuleWithWildcardPath.class,
                "failingRuleWithWildcardPath" );

        gen.get().setSection("ops");

        functionalTestHelper = new FunctionalTestHelper( server );

        JaxRsResponse clientResponse = gen.get()
                .expectedStatus( 401 )
                .expectedType( MediaType.APPLICATION_JSON_TYPE )
                .expectedHeader( "WWW-Authenticate" )
                .get( trimTrailingSlash( functionalTestHelper.baseUri() )
                        + mountPoint + "/more/stuff" )
                .response();

        assertEquals(401, clientResponse.getStatus());
    }

    @Test
    @Title( "Using Complex Wildcards to Target Security Rules" )
    @Documented( "In this example, a security rule is registered to deny\n" +
                 "access to all URIs matching a complex pattern.\n" +
                 "The config looks like this:\n" +
                 "\n" +
                 "@@config\n" +
                 "\n" +
                 "with the rule source code of:\n" +
                 "\n" +
                 "@@failingRuleWithComplexWildcardPath" )
    public void aComplexWildcardUriPathShould401OnAccessToProtectedSubPath()
            throws Exception
    {
        String mountPoint = "/protected/wildcard_replacement/x/y/z/something/else/more_wildcard_replacement/a/b/c" +
                "/final/bit";
        server = CommunityServerBuilder.server().withDefaultDatabaseTuning()
                .withThirdPartyJaxRsPackage( "org.dummy.web.service",
                        mountPoint )
                .withSecurityRules(
                        PermanentlyFailingSecurityRuleWithComplexWildcardPath.class.getCanonicalName() )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        gen.get().addSnippet(
                "config",
                "\n[source,properties]\n----\norg.neo4j.server.rest.security_rules=my.rules" +
                        ".PermanentlyFailingSecurityRuleWithComplexWildcardPath\n----\n");
        gen.get().addTestSourceSnippets( PermanentlyFailingSecurityRuleWithComplexWildcardPath.class,
                "failingRuleWithComplexWildcardPath" );
        gen.get().setSection( "ops" );

        functionalTestHelper = new FunctionalTestHelper( server );

        JaxRsResponse clientResponse = gen.get()
                .expectedStatus( 401 )
                .expectedType( MediaType.APPLICATION_JSON_TYPE )
                .expectedHeader( "WWW-Authenticate" )
                .get( trimTrailingSlash( functionalTestHelper.baseUri() )
                        + mountPoint + "/more/stuff" )
                .response();

        assertEquals( 401, clientResponse.getStatus() );
    }


    @Test
    public void should403WhenAuthenticatedButForbidden()
            throws Exception
    {
        server = CommunityServerBuilder.server().withDefaultDatabaseTuning().withSecurityRules(
                PermanentlyForbiddenSecurityRule.class.getCanonicalName(),
                PermanentlyPassingSecurityRule.class.getCanonicalName() )
                .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );

        JaxRsResponse clientResponse = gen.get()
                .expectedStatus(403)
                .expectedType(MediaType.APPLICATION_JSON_TYPE)
                .get(trimTrailingSlash(functionalTestHelper.baseUri()))
                .response();

        assertEquals(403, clientResponse.getStatus());
    }

    private String trimTrailingSlash( URI uri )
    {
        String result = uri.toString();
        if ( result.endsWith( "/" ) )
        {
            return result.substring( 0, result.length() - 1 );
        }
        else
        {
            return result;
        }
    }
}
