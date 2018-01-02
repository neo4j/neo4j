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

import org.apache.commons.configuration.Configuration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.Config;

/**
 * Used by the server to load server and database properties.
 * @deprecated this is no longer supported, public programmatic access to Neo4j Server is deprecated, internal
 *             config should use {@link Config}. This will be removed in the next major version of Neo4j.
 */
@Deprecated
public interface ConfigurationBuilder
{
    /**
     * @return the configuration to access server properties.
     */
    Config configuration();

    /**
     * @return the properties that are used by {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory a graph database} to build database properties.
     */
    Map<String, String> getDatabaseTuningProperties();

    /*
     * The wrapping classes are only used for support legacy code.
     * Once we are ready to move the deprecated classes in the server package into an internal package,
     * we should also then remove these wrapping classes too.
     */
    class ConfiguratorWrappingConfigurationBuilder implements ConfigurationBuilder
    {
        private final Config serverConfig;
        private final Map<String, String> dbProperties;

        public ConfiguratorWrappingConfigurationBuilder ( Configurator configurator )
        {
            // copy the server properties to create server config
            Map<String, String> serverProperties = new HashMap<>();
            Configuration oldConfiguration = configurator.configuration();
            Iterator<String> keys = oldConfiguration.getKeys();
            while( keys.hasNext() )
            {
                String key = keys.next();
                serverProperties.put( key, oldConfiguration.getProperty( key ).toString() );
            }
            serverProperties.put( ServerSettings.third_party_packages.name(),
                    toStringForThirdPartyPackageProperty( configurator.getThirdpartyJaxRsPackages() ) );

            this.serverConfig = new Config( serverProperties, BaseServerConfigLoader.getDefaultSettingsClasses() );
            // use the db properties directly
            this.dbProperties = configurator.getDatabaseTuningProperties();
        }

        @Override
        public Config configuration()
        {
            return this.serverConfig;
        }

        @Override
        public Map<String,String> getDatabaseTuningProperties()
        {
            return this.dbProperties;
        }

        public static String toStringForThirdPartyPackageProperty( List<ThirdPartyJaxRsPackage> extensions )
        {
            String propertyString = "";
            int packageCount = extensions.size();

            if( packageCount == 0 )
                return propertyString;
            else
            {
                ThirdPartyJaxRsPackage jaxRsPackage = null;
                for( int i = 0; i < packageCount - 1; i ++ )
                {
                    jaxRsPackage = extensions.get( i );
                    propertyString += jaxRsPackage.getPackageName() + "=" + jaxRsPackage.getMountPoint() + Settings.SEPARATOR;
                }
                jaxRsPackage = extensions.get( packageCount - 1 );
                propertyString += jaxRsPackage.getPackageName() + "=" + jaxRsPackage.getMountPoint();
                return propertyString;
            }
        }
    }

    class ConfigWrappingConfigurator extends Configurator.Adapter
    {
        private Config config;

        public ConfigWrappingConfigurator( Config config )
        {
            this.config = config;
        }

        @Override
        public Configuration configuration()
        {
            return new ConfigWrappingConfiguration( config );
        }

        @Override
        public Map<String,String> getDatabaseTuningProperties()
        {
            return config.getParams();
        }

        @Override
        public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsPackages()
        {
            return config.get( ServerSettings.third_party_packages );
        }

    }
}
