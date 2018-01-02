/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.web;

import java.io.File;
import java.net.URI;

import org.neo4j.graphdb.config.Setting;

import static java.io.File.separator;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.NORMALIZED_RELATIVE_URI;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.URI;
import static org.neo4j.kernel.configuration.Settings.setting;

/**
 *
 * The settings for internal use. Should not be visible to Server API
 *
 */
public class ServerInternalSettings
{
    /**
     * Key for the server configuration file. The file path should always be get/set using System.property.
     */
    public static final String SERVER_CONFIG_FILE_KEY = "org.neo4j.server.properties";

    /**
     * Path to the server configuration file. The file path should always be get/set using System.property.
     */
    public static final String SERVER_CONFIG_FILE = "config/neo4j-server.properties";

    /**
     *  Default name for the db configuration file.
     */
    public static final String DB_TUNING_CONFIG_FILE_NAME = "neo4j.properties";

    public static final Setting<Boolean> webserver_statistics_collection_enabled = setting(
            "org.neo4j.server.webserver.statistics", BOOLEAN, FALSE );

    // paths
    public static final Setting<URI> rest_api_path = setting( "org.neo4j.server.webadmin.data.uri",
            NORMALIZED_RELATIVE_URI, "/db/data" );

    public static final Setting<URI> management_api_path = setting( "org.neo4j.server.webadmin.management.uri",
            NORMALIZED_RELATIVE_URI, "/db/manage" );

    public static final Setting<URI> browser_path = setting( "org.neo4j.server.webadmin.browser.uri", URI, "/browser/" );

    public static final Setting<Boolean> script_sandboxing_enabled = setting("org.neo4j.server.script.sandboxing.enabled",
            BOOLEAN, TRUE );

    public static final Setting<Boolean> wadl_enabled = setting( "unsupported_wadl_generation_enabled", BOOLEAN,
            FALSE );

    public static final Setting<Long> startup_timeout = setting( "org.neo4j.server.startup_timeout", DURATION, "120s" );

    public static final Setting<File> auth_store = setting("dbms.security.auth_store.location", PATH, "data/dbms/auth");

    public static final Setting<File> rrd_store = setting("org.neo4j.server.webadmin.rrdb.location", PATH, "data/rrd");

    public static final Setting<File> legacy_db_location = setting( "org.neo4j.server.database.location", PATH, "data/graph.db" );

    public static final Setting<File> legacy_db_config = setting( "org.neo4j.server.db.tuning.properties", PATH,
            separator + "etc" + separator + "neo" + separator + ServerInternalSettings.DB_TUNING_CONFIG_FILE_NAME);

    public static final Setting<Boolean> webadmin_enabled = setting( "dbms.webadmin.enabled", BOOLEAN, TRUE );

    public static final Setting<Boolean> rrdb_enabled = setting( "dbms.rrdb.enabled", BOOLEAN, FALSE );
}
