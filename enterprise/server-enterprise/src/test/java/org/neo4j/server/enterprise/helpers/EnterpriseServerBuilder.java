/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.enterprise.EnterpriseNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.web.DatabaseActions;

import static org.neo4j.helpers.ListenSocketAddress.listenAddress;

public class EnterpriseServerBuilder extends CommunityServerBuilder
{
    protected EnterpriseServerBuilder( LogProvider logProvider )
    {
        super( logProvider );
    }

    public static EnterpriseServerBuilder server()
    {
        return server( NullLogProvider.getInstance() );
    }

    public static EnterpriseServerBuilder serverOnRandomPorts()
    {
        EnterpriseServerBuilder server = server();
        server.onRandomPorts();
        server.withProperty( new BoltConnector( "bolt" ).listen_address.name(), "localhost:0" );
        server.withProperty( OnlineBackupSettings.online_backup_server.name(),
                listenAddress( "127.0.0.1", PortAuthority.allocatePort() ) );
        return server;
    }

    public static EnterpriseServerBuilder server( LogProvider logProvider )
    {
        return new EnterpriseServerBuilder( logProvider );
    }

    @Override
    public EnterpriseNeoServer build() throws IOException
    {
        return (EnterpriseNeoServer) super.build();
    }

    @Override
    public EnterpriseServerBuilder usingDataDir( String dataDir )
    {
        super.usingDataDir( dataDir );
        return this;
    }

    @Override
    protected CommunityNeoServer build( File configFile, Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        return new TestEnterpriseNeoServer( config, configFile, dependencies, logProvider );
    }

    private class TestEnterpriseNeoServer extends EnterpriseNeoServer
    {
        private final File configFile;

        TestEnterpriseNeoServer( Config config, File configFile,
                GraphDatabaseFacadeFactory.Dependencies dependencies, LogProvider logProvider )
        {
            super( config, dependencies, logProvider );
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
            if ( configFile != null )
            {
                configFile.delete();
            }
        }
    }

    @Override
    public Map<String, String> createConfiguration( File temporaryFolder )
    {
        Map<String, String> configuration = super.createConfiguration( temporaryFolder );

        configuration.put( OnlineBackupSettings.online_backup_server.name(), listenAddress( "127.0.0.1", PortAuthority.allocatePort() ) );

        return configuration;
    }
}
