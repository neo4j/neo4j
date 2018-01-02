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
package org.neo4j.dbms;

import java.io.File;

import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;

public class DatabaseManagementSystemSettings implements LoadableConfig
{
    @Internal
    public static final Setting<File> auth_store_directory = derivedSetting( "unsupported.dbms.directories.auth",
            GraphDatabaseSettings.data_directory,
            data -> new File( data, "dbms" ),
            PATH );
}
