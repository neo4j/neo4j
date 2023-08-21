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
package org.neo4j.server.modules;

import static java.util.Collections.singletonList;
import static org.neo4j.server.configuration.ServerSettings.http_access_control_allow_origin;
import static org.neo4j.server.web.Injectable.injectable;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.rest.discovery.DiscoveryService;
import org.neo4j.server.rest.web.AccessiblePathFilter;
import org.neo4j.server.rest.web.CorsFilter;
import org.neo4j.server.web.WebServer;

/**
 * Mounts the DBMS REST API.
 */
public class DBMSModule implements ServerModule {
    private static final String ROOT_PATH = "/";

    private final WebServer webServer;
    private final Config config;
    private final Supplier<DiscoverableURIs> discoverableURIs;
    private final InternalLogProvider logProvider;
    private final AuthConfigProvider authConfigProvider;

    public DBMSModule(
            WebServer webServer,
            Config config,
            Supplier<DiscoverableURIs> discoverableURIs,
            InternalLogProvider logProvider,
            AuthConfigProvider authConfigProvider) {
        this.webServer = webServer;
        this.config = config;
        this.discoverableURIs = discoverableURIs;
        this.logProvider = logProvider;
        this.authConfigProvider = authConfigProvider;
    }

    @Override
    public void start() {
        webServer.addJAXRSClasses(
                singletonList(DiscoveryService.class),
                ROOT_PATH,
                List.of(
                        injectable(DiscoverableURIs.class, discoverableURIs.get()),
                        injectable(AuthConfigProvider.class, authConfigProvider)));

        webServer.addJAXRSClasses(jaxRsClasses(), ROOT_PATH, null);

        // add filters:
        webServer.addFilter(new CorsFilter(logProvider, config.get(http_access_control_allow_origin)), "/*");
        webServer.addFilter(
                new AccessiblePathFilter(logProvider, config.get(ServerSettings.http_paths_blacklist)), "/*");
    }

    @Override
    public void stop() {
        webServer.removeJAXRSClasses(jaxRsClasses(), ROOT_PATH);
    }

    private List<Class<?>> jaxRsClasses() {
        if (config.get(GraphDatabaseSettings.auth_enabled)) {
            return singletonList(JacksonJsonProvider.class);
        }
        return Collections.emptyList();
    }
}
