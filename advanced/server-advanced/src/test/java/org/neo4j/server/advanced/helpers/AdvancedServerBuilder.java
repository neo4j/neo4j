/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.advanced.helpers;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.advanced.AdvancedNeoServer;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.helpers.LoggingFactory;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.rest.web.DatabaseActions;

import static org.neo4j.server.database.LifecycleManagingDatabase.EMBEDDED;
import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;
import static org.neo4j.server.helpers.LoggingFactory.IMPERMANENT_LOGGING;
import static org.neo4j.server.helpers.LoggingFactory.given;

public class AdvancedServerBuilder extends CommunityServerBuilder
{
    private AdvancedServerBuilder( LoggingFactory loggingFactory )
    {
        super( loggingFactory );
    }

    public AdvancedServerBuilder( Logging logging )
    {
        this( given( logging ) );
    }

    public static AdvancedServerBuilder server( Logging logging )
    {
        return new AdvancedServerBuilder( logging );
    }

    public static AdvancedServerBuilder server()
    {
        return new AdvancedServerBuilder( IMPERMANENT_LOGGING );
    }

    @Override
    public AdvancedNeoServer build() throws IOException
    {
        return (AdvancedNeoServer) super.build();
    }

    @Override
    protected AdvancedNeoServer build(File configFile, ConfigurationBuilder configurator, InternalAbstractGraphDatabase.Dependencies dependencies)
    {
        return new TestAdvancedNeoServer( configurator, configFile, dependencies );
    }

    private class TestAdvancedNeoServer extends AdvancedNeoServer
    {
        private final File configFile;

        public TestAdvancedNeoServer( ConfigurationBuilder propertyFileConfigurator, File configFile, InternalAbstractGraphDatabase.Dependencies dependencies )
        {
            super( propertyFileConfigurator, lifecycleManagingDatabase( persistent ? EMBEDDED : IN_MEMORY_DB ), dependencies );
            this.configFile = configFile;
        }

        @Override
        protected PreFlightTasks createPreflightTasks()
        {
            return preflightTasks;
        }

        @Override
        protected DatabaseActions createDatabaseActions()
        {
            return createDatabaseActionsObject( database, configurator );
        }

        @Override
        public void stop()
        {
            super.stop();
            configFile.delete();
        }
    }
}
