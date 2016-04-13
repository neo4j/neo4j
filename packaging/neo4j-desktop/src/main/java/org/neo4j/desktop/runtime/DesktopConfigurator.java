/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.desktop.runtime;

import java.io.File;
import java.util.Optional;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.desktop.config.Installation;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLog;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.configuration.ServerSettings.HttpConnector;

import static org.neo4j.helpers.collection.Pair.pair;

public class DesktopConfigurator
{
    private final Installation installation;

    private Config config;
    private File dbDir;

    public DesktopConfigurator( Installation installation, File databaseDirectory )
    {
        this.installation = installation;
        this.dbDir = databaseDirectory;
        refresh();
    }

    public void refresh()
    {
        config = new ConfigLoader( CommunityBootstrapper.settingsClasses).loadConfig(
                Optional.of( installation.getConfigurationsFile() ),
                FormattedLog.toOutputStream( System.out ),
                (settings) -> settings.put( GraphDatabaseSettings.neo4j_home.name(), dbDir.getAbsolutePath() ),
                pair( DatabaseManagementSystemSettings.database_path.name(), dbDir.getAbsolutePath() ) );
    }

    public Config configuration()
    {
        return config;
    }

    public void setDatabaseDirectory( File directory )
    {
        dbDir = directory;
    }

    public String getDatabaseDirectory()
    {
        return dbDir.getAbsolutePath();
    }

    public HostnamePort getServerAddress()
    {
        return ServerSettings.httpConnector( config, HttpConnector.Encryption.NONE ).get().address.from( config );
    }
}
