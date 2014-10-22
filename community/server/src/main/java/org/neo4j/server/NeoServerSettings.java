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
import org.neo4j.server.web.ServerInternalSettings;

import static org.neo4j.helpers.Settings.PATH;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.setting;

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
    public static final Setting<String> legacy_db_mode = setting( "org.neo4j.server.database.mode", STRING, "SINGLE" );

    /**
     * The path to the server configuration file should not be configurable as it is used before {@link Configuration configuration} is created.
     * The user should rely on the default location of the server configuration file.
     */
    @Deprecated
    public static final Setting<File> server_config_file = setting( ServerInternalSettings.SERVER_CONFIG_FILE_KEY, PATH, ServerInternalSettings.SERVER_CONFIG_FILE );

    /**
     *  This is a duplicated setting of GraphDatabase.store_dir. The {@link GraphDatabaseSettings#store_dir} is the real one which is in use by db now.
     *  Changing this value might not change the db location.
     */
    @Deprecated
    @Title( "Legacy database location" )
    @Description( "Defines the location of the 'db' database. This is not the recommended way of controlling this " +
                  "anymore. Instead, specify a location, or rely on the default, when creating databases." )
    public static final Setting<File> legacy_db_location = setting( "org.neo4j.server.database.location", PATH, "data/graph.db" );

    @Deprecated
    @Title( "Legacy database configuration" )
    @Description( "Location of the 'db' database configuration file. This is not the recommended way of controlling " +
                  "this anymore. Instead, specify configuration for this database via the hosting API." )
    public static final Setting<File> legacy_db_config = setting( "org.neo4j.server.db.tuning.properties",
            PATH, File.separator + "etc" + File.separator + "neo" + File.separator + ServerInternalSettings.DB_TUNING_CONFIG_FILE_NAME );
}
