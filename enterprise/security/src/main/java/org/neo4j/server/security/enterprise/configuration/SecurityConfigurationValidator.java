/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.configuration;


import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.kernel.configuration.ConfigurationValidator;
import org.neo4j.logging.Log;


public class SecurityConfigurationValidator implements ConfigurationValidator
{
    @Override
    public Map<String,String> validate( Collection<SettingValidator> settingValidators, Map<String,String> rawConfig,
            Log log ) throws InvalidSettingException
    {
        String provider = SecuritySettings.auth_provider.apply( rawConfig::get );
        List<String> providers = SecuritySettings.auth_providers.apply( rawConfig::get );

        validateServerId( provider, providers );

        return rawConfig;
    }

    private static void validateServerId( String provider, List<String> providers )
    {
        if ( providers.size() > 1 || !providers.contains( provider ) )
        {
            throw new InvalidSettingException( String.format( "Using both SecuritySettings.auth_provider and SecuritySettings.auth_providers and they " +
                    "do not match: auth_provider = %s , auth_provider = %s", provider, providers ) );
        }

    }
}
