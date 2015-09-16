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
package org.neo4j.desktop.runtime;

import java.io.File;

import org.neo4j.desktop.config.Installation;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLog;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigFactory;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.web.ServerInternalSettings;

import static org.neo4j.helpers.Pair.pair;
import static org.neo4j.server.configuration.ServerSettings.tls_certificate_file;
import static org.neo4j.server.configuration.ServerSettings.tls_key_file;
import static org.neo4j.server.web.ServerInternalSettings.auth_store;

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
        config = ServerConfigFactory.loadConfig(
                /** Future single file, neo4j.conf or similar */
                null,

                /** Server config file */
                installation.getServerConfigurationsFile(),

                /** Database tuning file */
                getDatabaseConfigurationFile(),

                FormattedLog.toOutputStream( System.out ),

                /** Desktop-specific config overrides */
                pair( auth_store.name(), new File( dbDir, "./dbms/auth" ).getAbsolutePath() ),
                pair( tls_certificate_file.name(), new File( dbDir, "./dbms/ssl/snakeoil.cert" ).getAbsolutePath() ),
                pair( tls_key_file.name(), new File( dbDir, "./dbms/ssl/snakeoil.key" ).getAbsolutePath() ),

                pair( Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDir.getAbsolutePath() ) );
    }

    public Config configuration()
    {
        return config;
    }

    public void setDatabaseDirectory( File directory ) {
        dbDir = directory;
    }

    public String getDatabaseDirectory() {
        return config.get( ServerInternalSettings.legacy_db_location ).getAbsolutePath();
    }

    public int getServerPort() {
        return config.get( ServerSettings.webserver_port );
    }

    public File getDatabaseConfigurationFile() {
        return new File( dbDir, Installation.NEO4J_PROPERTIES_FILENAME );
    }
}
