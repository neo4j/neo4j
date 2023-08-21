/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.server.configuration.ServerSettings.http_enabled_modules;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.GET;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.dummy.web.service.DummyThirdPartyWebService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.rest.security.CommunityWebContainerTestBase;
import org.neo4j.test.server.HTTP;

class NeoWebServerConfigurableModulesIT extends CommunityWebContainerTestBase {
    @Test
    void webServerShouldNotStartWithoutAnyModule() {
        assertThatExceptionOfType(UnsatisfiedDependencyException.class).isThrownBy(() -> serverOnRandomPorts()
                .withProperty(http_enabled_modules.name(), "")
                .build());
    }

    @Test
    void authAndDbmsShouldBeEnabled() throws IOException {
        startTestWebContainer(EnumSet.of(ConfigurableServerModules.TRANSACTIONAL_ENDPOINTS), true);

        HTTP.Response response = POST(
                txCommitURL("system"), quotedJson("{ 'statements': [ { 'statement': 'SHOW DEFAULT DATABASE' } ] }"));
        assertThat(response.status()).isEqualTo(401);

        response = GET(testWebContainer.getBaseUri().resolve("/").toString());

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.<Map<String, Object>>content()).containsKey("transaction");
    }

    private static Stream<Arguments> disabledModuleAndURIs() {
        return Stream.of(
                arguments(
                        ConfigurableServerModules.TRANSACTIONAL_ENDPOINTS,
                        (Function<URI, List<URI>>) baseUir ->
                                List.of(baseUir.resolve("/db/neo4j/tx"), baseUir.resolve("/db/data/transaction")),
                        (Function<URI, HTTP.Response>) uri -> HTTP.POST(uri.toString())),
                arguments(
                        ConfigurableServerModules.BROWSER,
                        (Function<URI, List<URI>>) baseUir -> List.of(baseUir.resolve("/browser")),
                        (Function<URI, HTTP.Response>) uri -> HTTP.GET(uri.toString())),
                arguments(
                        ConfigurableServerModules.UNMANAGED_EXTENSIONS,
                        (Function<URI, List<URI>>) baseUir ->
                                List.of(baseUir.resolve(DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT)),
                        (Function<URI, HTTP.Response>) uri -> HTTP.withHeaders(HttpHeaders.ACCEPT, MediaType.TEXT_PLAIN)
                                .GET(uri.toString())));
    }

    @ParameterizedTest(name = "{0} should be disabled")
    @MethodSource("disabledModuleAndURIs")
    void moduleShouldBeDisabled(
            ConfigurableServerModules disabledModule,
            Function<URI, List<URI>> uriProvider,
            Function<URI, HTTP.Response> httpCall)
            throws IOException {
        startTestWebContainer(EnumSet.complementOf(EnumSet.of(disabledModule)), false);

        for (URI uri : uriProvider.apply(testWebContainer.getBaseUri())) {
            HTTP.Response response = httpCall.apply(uri);
            assertThat(response.status()).isEqualTo(404);
        }
    }

    private static String moduleSettingsToProperty(EnumSet<ConfigurableServerModules> enabledModules) {
        return enabledModules.stream().map(ConfigurableServerModules::name).collect(joining(","));
    }

    /**
     * @param enabledModules The modules that should be enabled
     * @param authEnabled    Whether auth is to be enabled or not
     * @throws IOException
     */
    void startTestWebContainer(EnumSet<ConfigurableServerModules> enabledModules, boolean authEnabled)
            throws IOException {
        testWebContainer = serverOnRandomPorts()
                .withProperty(GraphDatabaseSettings.auth_enabled.name(), Boolean.toString(authEnabled))
                .withProperty(http_enabled_modules.name(), moduleSettingsToProperty(enabledModules))
                .withThirdPartyJaxRsPackage(
                        "org.dummy.web.service", DummyThirdPartyWebService.DUMMY_WEB_SERVICE_MOUNT_POINT)
                .build();
    }
}
