/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.harness.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.MapBasedConfiguration;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;

public class MapConfigurator extends Configurator.Adapter
{
    private final Map<String, Object> config;
    private final List<ThirdPartyJaxRsPackage> extensions;

    public MapConfigurator( Map<String, Object> config, List<ThirdPartyJaxRsPackage> extensions )
    {
        this.config = config;
        this.extensions = extensions;
    }

    @Override
    public Map<String, String> getDatabaseTuningProperties()
    {
        return toStringStringMap(config);
    }

    private Map<String, String> toStringStringMap( Map<String, Object> config )
    {
        Map<String, String> converted = new HashMap<>();
        for ( Map.Entry<String, Object> entry : config.entrySet() )
        {
            converted.put( entry.getKey(), entry.getValue().toString() );
        }
        return converted;
    }

    @Override
    public Configuration configuration()
    {
        return new MapBasedConfiguration(config);
    }

    @Override
    public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses()
    {
        return extensions;
    }

    @Override
    public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsPackages()
    {
        return extensions;
    }
}
