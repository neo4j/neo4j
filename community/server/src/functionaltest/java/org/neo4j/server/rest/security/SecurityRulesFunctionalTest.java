/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.dummy.web.service.DummyThirdPartyWebService;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RESTDocsGenerator;
import org.neo4j.test.TestData;
import org.neo4j.test.TestData.Title;
import org.neo4j.test.server.ExclusiveServerTestBase;

import javax.ws.rs.core.MediaType;
import java.net.URI;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SecurityRulesFunctionalTest extends ExclusiveServerTestBase
{
    private NeoServerWithEmbeddedWebServer server;

    private FunctionalTestHelper functionalTestHelper;

    public
    @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough(RESTDocsGenerator.PRODUCER);

    @After
    public void stopServer()
    {
        if (server != null)
        {
            server.stop();
        }
    }

    /**
     * In this example, a (dummy) failing security rule is registered to deny
     * access to all URIs to the server by listing the rules class in
     * +neo4j-server.properties+:
     *
     * @@config with the rule source code of:
     * @@failingRule With this rule registered, any access to the server will be
     * denied. In a production-quality implementation the rule
     * will likely lookup credentials/claims in a 3rd party
     * directory service (e.g. LDAP) or in a local database of
     * authorized users.
     */
    @Test
    @Documented
    @Title("Enforcing Server Authorization Rules")
    public void should401WithBasicChallengeWhenASecurityRuleFails()
            throws Exception
    {
        server = ServerBuilder.server().withDefaultDatabaseTuning().withSecurityRules(
                PermanentlyFailingSecurityRule.class.getCanonicalName()).build();
        server.start();
        gen.get().addSnippet(
                "config",
                "\n[source]\n----\norg.neo4j.server.rest.security_rules=my.rules.PermanentlyFailingSecurityRule\n----\n");
        gen.get().addTestSourceSnippets(PermanentlyFailingSecurityRule.class,
                                        "failingRule");
        functionalTestHelper = new FunctionalTestHelper(server);
        gen.get().setSection("ops");
        JaxRsResponse response = gen.get().expectedStatus(401).expectedHeader(
                "WWW-Authenticate").post(functionalTestHelper.nodeUri()).response();

        assertThat(response.getHeaders().getFirst("WWW-Authenticate"),
                   containsString("Basic realm=\""
                                          + PermanentlyFailingSecurityRule.REALM + "\""));
    }

    @Test
    public void should401WithBasicChallengeIfAnyOneOfTheRulesFails()
            throws Exception
    {
        server = ServerBuilder.server().withDefaultDatabaseTuning().withSecurityRules(
                PermanentlyPassingSecurityRule.class.getCanonicalName(),
                PermanentlyFailingSecurityRule.class.getCanonicalName()).build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);

        JaxRsResponse response = gen.get().expectedStatus(401).expectedHeader(
                "WWW-Authenticate").post(functionalTestHelper.nodeUri()).response();

        assertThat(response.getHeaders().getFirst("WWW-Authenticate"),
                   containsString("Basic realm=\""
                                          + PermanentlyFailingSecurityRule.REALM + "\""));
    }

    @Test
    public void shouldRespondWith201IfAllTheRulesPassWhenCreatingANode()
            throws Exception
    {
        server = ServerBuilder.server().withDefaultDatabaseTuning().withSecurityRules(
                PermanentlyPassingSecurityRule.class.getCanonicalName()).build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);

        gen.get().expectedStatus(201).expectedHeader("Location").post(
                functionalTestHelper.nodeUri()).response();
    }

    @Test
    public void aSimpleWildcardUriPathShould401OnAccessToProtectedSubPath()
            throws Exception
    {
        String mountPoint = "/protected/tree/starts/here" + DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT;
        server = ServerBuilder.server().withDefaultDatabaseTuning()
                              .withThirdPartyJaxRsPackage("org.dummy.web.service",
                                                          mountPoint)
                              .withSecurityRules(
                                      PermanentlyFailingSecurityRuleWithWildcardPath.class.getCanonicalName())
                              .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);

        ClientResponse clientResponse = Client.create().resource(
                trimTrailingSlash(functionalTestHelper.baseUri()) + mountPoint + "/more/stuff").accept(
                MediaType.TEXT_PLAIN).get(ClientResponse.class);

        assertEquals(401, clientResponse.getStatus());
    }

    @Test
    public void aComplexWildcardUriPathShould401OnAccessToProtectedSubPath()
            throws Exception
    {
        String mountPoint = "/protected/wildcard_replacement/x/y/z/something/else/more_wildcard_replacement/a/b/c/final/bit";
        server = ServerBuilder.server().withDefaultDatabaseTuning()
                              .withThirdPartyJaxRsPackage("org.dummy.web.service",
                                                          mountPoint)
                              .withSecurityRules(
                                      PermanentlyFailingSecurityRuleWithComplexWildcardPath.class.getCanonicalName())
                              .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);

        ClientResponse clientResponse = Client.create().resource(
                trimTrailingSlash(functionalTestHelper.baseUri()) + mountPoint + "/more/stuff").accept(
                MediaType.TEXT_PLAIN).get(ClientResponse.class);

        assertEquals(401, clientResponse.getStatus());
    }

    private String trimTrailingSlash(URI uri)
    {
        String result = uri.toString();
        if (result.endsWith("/"))
        {
            return result.substring(0, result.length() - 1);
        }
        else
        {
            return result;
        }
    }
}
