/**
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
package org.neo4j.server.enterprise.helpers;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.EphemeralDatabase;
import org.neo4j.server.enterprise.EnterpriseNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.helpers.LoggingFactory;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.rest.web.DatabaseActions;

import static org.neo4j.server.helpers.LoggingFactory.IMPERMANENT_LOGGING;

public class EnterpriseServerBuilder extends CommunityServerBuilder
{
    protected EnterpriseServerBuilder( LoggingFactory loggingFactory )
    {
        super( loggingFactory );
    }

    public static EnterpriseServerBuilder server()
    {
        return server( IMPERMANENT_LOGGING );
    }

    public static EnterpriseServerBuilder server( LoggingFactory loggingFactory )
    {
        return new EnterpriseServerBuilder( loggingFactory );
    }

    @Override
    public EnterpriseNeoServer build() throws IOException
    {
        return (EnterpriseNeoServer) super.build();
    }

    @Override
    public EnterpriseNeoServer build( File configFile, Configurator configurator, Logging logging )
    {
        return new TestEnterpriseNeoServer( configurator, configFile, logging );
    }

    private class TestEnterpriseNeoServer extends EnterpriseNeoServer
    {
        private final File configFile;

        public TestEnterpriseNeoServer( Configurator propertyFileConfigurator, File configFile, Logging logging )
        {
            super( propertyFileConfigurator, logging );
            this.configFile = configFile;
        }

        @Override
        protected PreFlightTasks createPreflightTasks()
        {
            return preflightTasks;
        }

        @Override
        protected Database createDatabase()
        {
            return persistent ?
                    super.createDatabase() :
                    new EphemeralDatabase( configurator );
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
