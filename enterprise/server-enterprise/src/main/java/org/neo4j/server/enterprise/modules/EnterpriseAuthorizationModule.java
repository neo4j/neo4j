/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.enterprise.modules;

import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.rest.dbms.AuthorizationDisabledFilter;
import org.neo4j.server.rest.dbms.EnterpriseAuthorizationDisabledFilter;
import org.neo4j.server.web.WebServer;

public class EnterpriseAuthorizationModule extends AuthorizationModule
{
    public EnterpriseAuthorizationModule( WebServer webServer,
            Supplier<AuthManager> authManager,
            LogProvider logProvider, Config config,
            Pattern[] uriWhitelist )
    {
        super( webServer, authManager, logProvider, config, uriWhitelist );
    }

    @Override
    protected AuthorizationDisabledFilter createAuthorizationDisabledFilter()
    {
        return new EnterpriseAuthorizationDisabledFilter();
    }
}
