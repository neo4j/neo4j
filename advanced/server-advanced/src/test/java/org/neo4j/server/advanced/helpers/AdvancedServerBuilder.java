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
package org.neo4j.server.advanced.helpers;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.advanced.AdvancedNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.web.DatabaseActions;

import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class AdvancedServerBuilder extends CommunityServerBuilder
{
    private AdvancedServerBuilder( LogProvider userLogProvider )
    {
        super( userLogProvider );
    }

    public static AdvancedServerBuilder server( LogProvider userLogProvider )
    {
        return new AdvancedServerBuilder( userLogProvider );
    }

    public static AdvancedServerBuilder server()
    {
        return new AdvancedServerBuilder( NullLogProvider.getInstance() );
    }

    @Override
    public AdvancedNeoServer build() throws IOException
    {
        return (AdvancedNeoServer) super.build();
    }

    @Override
    protected AdvancedNeoServer build(File configFile, Config config, GraphDatabaseFacadeFactory.Dependencies dependencies)
    {
        return new TestAdvancedNeoServer( config, configFile, dependencies, logProvider );
    }

    private class TestAdvancedNeoServer extends AdvancedNeoServer
    {
        private final File configFile;

        public TestAdvancedNeoServer( Config config, File configFile, GraphDatabaseFacadeFactory.Dependencies dependencies, LogProvider
                logProvider )
        {
            super( config, lifecycleManagingDatabase( persistent ? COMMUNITY_FACTORY : IN_MEMORY_DB ), dependencies, logProvider );
            this.configFile = configFile;
        }

        @Override
        protected DatabaseActions createDatabaseActions()
        {
            return createDatabaseActionsObject( database, getConfig() );
        }

        @Override
        public void stop()
        {
            super.stop();
            configFile.delete();
        }
    }
}
