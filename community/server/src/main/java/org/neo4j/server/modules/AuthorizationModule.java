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
package org.neo4j.server.modules;

import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.servlet.Filter;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.rest.dbms.AuthorizationDisabledFilter;
import org.neo4j.server.rest.dbms.AuthorizationEnabledFilter;
import org.neo4j.server.web.WebServer;

public class AuthorizationModule implements ServerModule
{
    private final WebServer webServer;
    private final Config config;
    private final Supplier<AuthManager> authManagerSupplier;
    private final LogProvider logProvider;
    private final Pattern[] uriWhitelist;

    public AuthorizationModule( WebServer webServer, Supplier<AuthManager> authManager, LogProvider logProvider,
            Config config, Pattern[] uriWhitelist )
    {
        this.webServer = webServer;
        this.config = config;
        this.authManagerSupplier = authManager;
        this.logProvider = logProvider;
        this.uriWhitelist = uriWhitelist;
    }

    @Override
    public void start()
    {
        final Filter authorizationFilter;

        if ( config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            authorizationFilter = new AuthorizationEnabledFilter( authManagerSupplier, logProvider, uriWhitelist );
        }
        else
        {
            authorizationFilter = createAuthorizationDisabledFilter();
        }

        webServer.addFilter( authorizationFilter, "/*" );
    }

    @Override
    public void stop()
    {
    }

    protected AuthorizationDisabledFilter createAuthorizationDisabledFilter()
    {
        return new AuthorizationDisabledFilter();
    }
}
