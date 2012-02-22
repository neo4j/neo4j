/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.kernel.ha;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.ConfigurationMigrator;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * TODO
 */
public class EnterpriseConfigurationMigrator
    extends ConfigurationMigrator
{
    public EnterpriseConfigurationMigrator( StringLogger messageLog )
    {
        super(messageLog);
    }

    public Map<String, String> migrateConfiguration( Map< String, String> inputParams )
    {
        Map<String, String> migratedConfiguration = new HashMap<String, String>( );
        
        for( Map.Entry<String, String> configEntry : super.migrateConfiguration( inputParams ).entrySet() )
        {
            String key = configEntry.getKey();
            String value = configEntry.getValue();

            if (key.equals( "ha.machine_id" ))
            {
                migratedConfiguration.put( "ha.server_id", value );
                deprecationMessage( "ha.machine_id has been replaced with ha.server_id" );
                continue;
            }

            if (key.equals( "ha.zoo_keeper_servers" ))
            {
                migratedConfiguration.put( "ha.coordinators", value );
                deprecationMessage( "ha.zoo_keeper_servers has been replaced with ha.coordinators" );
                continue;
            }

            migratedConfiguration.put( key, value );
        }
        
        return migratedConfiguration;
    }
}
