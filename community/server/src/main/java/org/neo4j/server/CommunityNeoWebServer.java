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

import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIs;

import java.util.ArrayList;
import java.util.function.Supplier;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.routing.ClientRoutingDomainChecker;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPools;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.Neo4jBrowserModule;
import org.neo4j.server.modules.QueryModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.modules.TransactionModule;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.web.JettyWebServer;
import org.neo4j.server.web.WebServer;
import org.neo4j.time.SystemNanoClock;

public class CommunityNeoWebServer extends AbstractNeoWebServer {
    public CommunityNeoWebServer(
            DatabaseManagementService managementService,
            TransactionManager transactionManager,
            Dependencies globalDependencies,
            Config config,
            InternalLogProvider userLogProvider,
            DbmsInfo dbmsInfo,
            MemoryPools memoryPools,
            SystemNanoClock clock) {
        super(
                managementService,
                globalDependencies,
                config,
                userLogProvider,
                dbmsInfo,
                memoryPools,
                transactionManager,
                clock);
    }

    @Override
    protected Iterable<ServerModule> createServerModules() {
        var config = getConfig();
        var enabledModules = config.get(ServerSettings.http_enabled_modules);
        var serverModules = new ArrayList<ServerModule>();
        if (!enabledModules.isEmpty()) {
            serverModules.add(createDBMSModule());

            if (enabledModules.contains(ConfigurableServerModules.TRANSACTIONAL_ENDPOINTS)) {
                serverModules.add(new TransactionModule(webServer, config, clock));
            }
            if (enabledModules.contains(ConfigurableServerModules.UNMANAGED_EXTENSIONS)) {
                serverModules.add(new ThirdPartyJAXRSModule(webServer, config, userLogProvider));
            }
            if (enabledModules.contains(ConfigurableServerModules.BROWSER)) {
                serverModules.add(new Neo4jBrowserModule(webServer));
            }
            if (enabledModules.contains(ConfigurableServerModules.QUERY_API_ENDPOINTS)) {
                serverModules.add(new QueryModule(webServer, config));
            }

            serverModules.add(createAuthorizationModule());
        }
        return serverModules;
    }

    @Override
    protected WebServer createWebServer() {
        var globalDependencies = getGlobalDependencies();
        var connectionTracker = globalDependencies.resolveDependency(NetworkConnectionTracker.class);
        var webServer = new JettyWebServer(userLogProvider, getConfig(), connectionTracker, byteBufferPool);
        globalDependencies.satisfyDependency(webServer);
        return webServer;
    }

    protected DBMSModule createDBMSModule() {
        var globalDependencies = getGlobalDependencies();
        var clientRoutingDomainChecker = globalDependencies.resolveDependency(ClientRoutingDomainChecker.class);
        // Bolt port isn't available until runtime, so defer loading until then
        Supplier<DiscoverableURIs> discoverableURIs =
                () -> communityDiscoverableURIs(getConfig(), connectorPortRegister, clientRoutingDomainChecker);
        var authConfigProvider = getGlobalDependencies().resolveDependency(AuthConfigProvider.class);
        return new DBMSModule(webServer, getConfig(), discoverableURIs, userLogProvider, authConfigProvider);
    }

    protected AuthorizationModule createAuthorizationModule() {
        return new AuthorizationModule(webServer, authManagerSupplier, userLogProvider, getConfig(), getUriWhitelist());
    }
}
