/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.enterprise;

import java.io.File;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.logging.Log;
import org.neo4j.server.configuration.BaseServerConfigLoader;

import static java.util.Arrays.asList;

public class EnterpriseServerConfigLoader extends BaseServerConfigLoader
{
    @Override
    public Config loadConfig( File configFile, File legacyConfigFile, Log log, Pair<String,String>... configOverrides )
    {
        Config config = super.loadConfig( configFile, legacyConfigFile, log, configOverrides );
        if ( config.get( EnterpriseServerSettings.mode ).equals( "HA" ) )
        {
            config.registerSettingsClasses( asList( HaSettings.class, ClusterSettings.class ) );
        }
        return config;
    }
}
