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
package org.neo4j.server.helpers;

import java.net.URI;
import java.util.Optional;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.NeoWebServer;
import org.neo4j.server.http.cypher.TransactionRegistry;

import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

public class TestWebContainer
{
    private final DatabaseManagementService managementService;
    private final NeoWebServer neoWebServer;

    public TestWebContainer( DatabaseManagementService managementService )
    {
        requireNonNull( managementService );
        this.managementService = managementService;
        this.neoWebServer = getNeoWebServer( managementService );
    }

    public URI getBaseUri()
    {
        return neoWebServer.getBaseUri();
    }

    public Optional<URI> httpsUri()
    {
        return neoWebServer.httpsUri();
    }

    public void shutdown()
    {
        managementService.shutdown();
    }

    public DatabaseManagementService getDatabaseManagementService()
    {
        return managementService;
    }

    public GraphDatabaseFacade getDefaultDatabase()
    {
        var config = getDependencyResolver( managementService ).resolveDependency( Config.class );
        var defaultDatabase = config.get( default_database );
        return (GraphDatabaseFacade) managementService.database( defaultDatabase );
    }

    public <T> T resolveDependency( Class<T> clazz )
    {
        return getDependencyResolver( managementService ).resolveDependency( clazz );
    }

    public Config getConfig()
    {
        return neoWebServer.getConfig();
    }

    public TransactionRegistry getTransactionRegistry()
    {
        return neoWebServer.getTransactionRegistry();
    }

    private NeoWebServer getNeoWebServer( DatabaseManagementService managementService )
    {
        return getDependencyResolver( managementService ).resolveDependency( NeoWebServer.class );
    }

    private DependencyResolver getDependencyResolver( DatabaseManagementService managementService )
    {
        return ((GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME )).getDependencyResolver();
    }
}
