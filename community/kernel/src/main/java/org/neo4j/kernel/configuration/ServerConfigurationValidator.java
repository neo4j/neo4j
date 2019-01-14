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
package org.neo4j.kernel.configuration;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.HTTP;


public class ServerConfigurationValidator implements ConfigurationValidator
{
    /**
     * Verifies that at least one http connector is specified and enabled.
     */
    @Override
    public Map<String,String> validate( @Nonnull Config config, @Nonnull Log log ) throws InvalidSettingException
    {
        Map<String,String> validSettings = new HashMap<>();

        // Add missing type info -- validation has succeeded so we can do this with confidence
        boolean hasEnabledConnector = false;
        for ( String identifier : config.identifiersFromGroup( Connector.class ) )
        {
            Connector connector = new Connector( identifier );
            if ( "http".equalsIgnoreCase( connector.group.groupKey ) || "https".equalsIgnoreCase( connector.group.groupKey ) )
            {
                validSettings.put( connector.type.name(), HTTP.name() );
                hasEnabledConnector = hasEnabledConnector ? true : config.get( connector.enabled );
            }
            else
            {
                validSettings.put( connector.type.name(), BOLT.name() );
            }
        }

        if ( !hasEnabledConnector )
        {
            throw new InvalidSettingException( String.format( "Missing mandatory enabled connector of type '%s'", HTTP ) );
        }

        return validSettings;
    }
}
