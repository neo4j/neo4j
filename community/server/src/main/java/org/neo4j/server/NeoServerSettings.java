/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server;

import java.io.File;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.kernel.configuration.Title;

import static org.neo4j.helpers.Settings.PATH;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.setting;
import static org.neo4j.server.configuration.Configurator.DATABASE_LOCATION_PROPERTY_KEY;
import static org.neo4j.server.configuration.Configurator.DB_MODE_KEY;
import static org.neo4j.server.configuration.Configurator.DB_TUNING_PROPERTY_FILE_KEY;
import static org.neo4j.server.configuration.Configurator.DEFAULT_CONFIG_DIR;
import static org.neo4j.server.configuration.Configurator.DEFAULT_DATABASE_LOCATION_PROPERTY_KEY;
import static org.neo4j.server.configuration.Configurator.NEO_SERVER_CONFIG_FILE_KEY;

@Description("Settings for the Neo4j Server")
public class NeoServerSettings
{
    @Title("Config database location")
    @Description("Path to where the server stores its configuration database.")
    public static final Setting<File> config_db_path = setting( "config_db.path", PATH, "data/__config__.db" );

    @Deprecated
    @Title("Legacy database mode")
    @Description("Defines the operation mode of the 'db' database - single or HA. This is not the recommended way of " +
            "controlling this anymore. Instead, specify a provider (single or ha) when creating databases.")
    public static final Setting<String> legacy_db_mode = setting( DB_MODE_KEY, STRING, "SINGLE");

    public static final Setting<File> server_config_file = setting( NEO_SERVER_CONFIG_FILE_KEY, PATH, "config/neo4j-server.properties" );

    @Deprecated
    @Title( "Legacy database location" )
    @Description( "Defines the location of the 'db' database. This is not the recommended way of controlling this " +
                  "anymore. Instead, specify a location, or rely on the default, when creating databases." )
    public static final Setting<File> legacy_db_location = setting( DATABASE_LOCATION_PROPERTY_KEY, PATH, DEFAULT_DATABASE_LOCATION_PROPERTY_KEY );


    @Deprecated
    @Title( "Legacy database configuration" )
    @Description( "Location of the 'db' database configuration file. This is not the recommended way of controlling " +
                  "this anymore. Instead, specify configuration for this database via the hosting API." )
    public static final Setting<File> legacy_db_config = setting( DB_TUNING_PROPERTY_FILE_KEY, PATH,
            new File( new File( System.getProperty( NEO_SERVER_CONFIG_FILE_KEY, DEFAULT_CONFIG_DIR ) ).getParentFile(),
                      "neo4j.properties" ).getAbsolutePath());

}
