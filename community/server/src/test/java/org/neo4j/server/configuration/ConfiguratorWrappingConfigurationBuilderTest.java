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
package org.neo4j.server.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.server.configuration.ConfigurationBuilder.ConfiguratorWrappingConfigurationBuilder;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfiguratorWrappingConfigurationBuilderTest
{
    @Test
    public void shouldGetNewPropertyValuesFromConfiguratorWrappingBuilder() throws Exception
    {
        // GIVEN
        Configurator configurator = new Configurator.Adapter()
        {
            @Override
            public Configuration configuration()
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( ServerSettings.http_logging_enabled.name(), new Boolean( true ) );
                return new MapConfiguration( properties );
            }

            @Override
            public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsPackages()
            {
                List<ThirdPartyJaxRsPackage> list = new ArrayList();
                list.add( new ThirdPartyJaxRsPackage( "lala", "sasa" ) );
                return list;
            }
        };

        // WHEN
        ConfiguratorWrappingConfigurationBuilder builder = new ConfiguratorWrappingConfigurationBuilder( configurator );

        // THEN
        assertTrue( builder.configuration().get( ServerSettings.http_logging_enabled ) );
        assertEquals( 1, builder.configuration().get( ServerSettings.third_party_packages ).size() );
    }
}
