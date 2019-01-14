/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.enterprise.helpers;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.enterprise.OpenEnterpriseNeoServer;
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
    public OpenEnterpriseNeoServer build() throws IOException
    {
        return (OpenEnterpriseNeoServer) super.build();
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

    private class TestEnterpriseNeoServer extends OpenEnterpriseNeoServer
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
        configuration.putIfAbsent( MetricsSettings.csvPath.name(), new File( temporaryFolder, "metrics" ).getAbsolutePath() );

        return configuration;
    }
}
