/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.configuration;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.ServerTestUtils;

public class ConfigFileBuilder
{
    private final File directory;
    private final Map<String,String> config;

    public static ConfigFileBuilder builder( File directory )
    {
        return new ConfigFileBuilder( directory );
    }

    private ConfigFileBuilder( File directory )
    {
        this.directory = directory;

        //initialize config with defaults that doesn't pollute
        //workspace with generated data
        this.config = MapUtil.stringMap(
                GraphDatabaseSettings.data_directory.name(), directory.getAbsolutePath() + "/data",
                ServerSettings.management_api_path.name(), "http://localhost:7474/db/manage/",
                ServerSettings.rest_api_path.name(), "http://localhost:7474/db/data/" );
    }

    public File build()
    {
        File file = new File( directory, "config" );
        ServerTestUtils.writeConfigToFile( config, file );
        return file;
    }

    public ConfigFileBuilder withNameValue( String name, String value )
    {
        config.put( name, value );
        return this;
    }

    public ConfigFileBuilder withSetting( Setting setting, String value )
    {
        config.put( setting.name(), value );
        return this;
    }

    public ConfigFileBuilder withoutSetting( Setting setting )
    {
        config.remove( setting.name() );
        return this;
    }
}
