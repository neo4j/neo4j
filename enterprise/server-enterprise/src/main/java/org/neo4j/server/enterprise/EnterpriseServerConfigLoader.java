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
package org.neo4j.server.enterprise;

import java.util.HashMap;
import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.server.configuration.BaseServerConfigLoader;

import static java.util.Arrays.asList;

import static org.neo4j.server.enterprise.EnterpriseServerSettings.mode;

public class EnterpriseServerConfigLoader extends BaseServerConfigLoader
{
    @Override
    protected List<Class<?>> settingsClasses( HashMap<String, String> settings )
    {
        List<Class<?>> classes = super.settingsClasses( settings );
        if ( new Config( settings, EnterpriseServerSettings.class ).get( mode ).equals( "HA" ) )
        {
            classes.addAll( asList( HaSettings.class, ClusterSettings.class ) );
        }
        return classes;
    }
}
