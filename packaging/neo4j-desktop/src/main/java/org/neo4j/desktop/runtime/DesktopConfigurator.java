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

import org.neo4j.desktop.config.Installation;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLog;
import org.neo4j.server.configuration.BaseServerConfigLoader;
import org.neo4j.server.configuration.ServerSettings;

import static org.neo4j.helpers.collection.Pair.pair;
import static org.neo4j.server.configuration.ServerSettings.auth_store;
import static org.neo4j.server.configuration.ServerSettings.tls_certificate_file;
import static org.neo4j.server.configuration.ServerSettings.tls_key_file;

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
        config = new BaseServerConfigLoader().loadConfig(
                /** Future single file, neo4j.conf or similar */
                null,

                /** Server config file */
                installation.getConfigurationsFile(),

                FormattedLog.toOutputStream( System.out ),

                /** Desktop-specific config overrides */
                pair( auth_store.name(), new File( dbDir, "./dbms/auth" ).getAbsolutePath() ),
                pair( tls_certificate_file.name(), new File( dbDir, "./dbms/ssl/snakeoil.cert" ).getAbsolutePath() ),
                pair( tls_key_file.name(), new File( dbDir, "./dbms/ssl/snakeoil.key" ).getAbsolutePath() ),

                pair( ServerSettings.database_path.name(), dbDir.getAbsolutePath() ) );
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

    public int getServerPort(){ return config.get( ServerSettings.webserver_port ); }

}
