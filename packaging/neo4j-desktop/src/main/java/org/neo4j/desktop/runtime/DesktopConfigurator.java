/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.desktop.runtime;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

import org.neo4j.desktop.config.Installation;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;

import static org.neo4j.helpers.collection.MapUtil.load;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class DesktopConfigurator implements Configurator
{
    private final CompositeConfiguration compositeConfig = new CompositeConfiguration();

    private final Map<String, String> map = new HashMap<>();
    private final Installation installation;

    private Configurator propertyFileConfig;

    public DesktopConfigurator( Installation installation )
    {
        this.installation = installation;
        refresh();
    }

    public void refresh() {
        compositeConfig.clear();

        compositeConfig.addConfiguration(new MapConfiguration( map ));

        // re-read server properties, then add to config
        propertyFileConfig = new PropertyFileConfigurator( installation.getServerConfigurationsFile() );
        compositeConfig.addConfiguration( propertyFileConfig.configuration() );
    }

    @Override
    public Configuration configuration()
    {
        return compositeConfig;
    }

    @Override
    public Map<String, String> getDatabaseTuningProperties()
    {
        try
        {
            return load( getDatabaseConfigurationFile() );
        }
        catch ( IOException e )
        {
            return stringMap();
        }
    }

    @Override
    public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses()
    {
        return propertyFileConfig.getThirdpartyJaxRsClasses();
    }

    @Override
    public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsPackages()
    {
        return propertyFileConfig.getThirdpartyJaxRsPackages();
    }

    public void setDatabaseDirectory( File directory ) {
        File neo4jProperties = new File( directory, Installation.NEO4J_PROPERTIES_FILENAME );

        map.put( Configurator.DATABASE_LOCATION_PROPERTY_KEY, directory.getAbsolutePath() );
        map.put( Configurator.DB_TUNING_PROPERTY_FILE_KEY, neo4jProperties.getAbsolutePath() );
    }

    public String getDatabaseDirectory() {
        return map.get( Configurator.DATABASE_LOCATION_PROPERTY_KEY );
    }

    public int getServerPort() {
        return configuration().getInt( Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT );
    }

    public File getDatabaseConfigurationFile() {
        return new File( map.get( Configurator.DB_TUNING_PROPERTY_FILE_KEY ) );
    }
}
