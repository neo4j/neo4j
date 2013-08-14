/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.neo4j.desktop.config.Value;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;

import static org.neo4j.helpers.collection.MapUtil.load;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * {@link Bootstrapper} that can start a Neo4j server with an optional config file contained within the
 * same directory as the database files.
 */
public class DesktopBootstrapper extends Bootstrapper
{
    private final File databaseLocation;
    private final File propertiesFileLocation;
    private final Value<List<String>> extensionPackages;

    public DesktopBootstrapper( File databaseLocation, File propertiesFileLocation,
            Value<List<String>> extensionPackages )
    {
        this.databaseLocation = databaseLocation;
        this.propertiesFileLocation = propertiesFileLocation;
        this.extensionPackages = extensionPackages;
    }

    @Override
    protected NeoServer createNeoServer()
    {
        return new CommunityNeoServer( createConfigurator() );
    }

    @Override
    protected Configurator createConfigurator()
    {
        final Map<String, String> map = new HashMap<String, String>();
        map.put( Configurator.DATABASE_LOCATION_PROPERTY_KEY, databaseLocation.getAbsolutePath() );
        
        List<String> packages = extensionPackages.get();
        if ( !packages.isEmpty() )
        {
            map.put( Configurator.THIRD_PARTY_PACKAGES_KEY, toCommaSeparatedString( packages ) );
        }

        return new Configurator()
        {
            @Override
            public Configuration configuration()
            {
                return new MapConfiguration( map );
            }

            @Override
            public Map<String, String> getDatabaseTuningProperties()
            {
                return loadDatabasePropertiesFromFileInDatabaseDirectoryIfExists();
            }

            @Override
            public Set<ThirdPartyJaxRsPackage> getThirdpartyJaxRsPackages()
            {
                return Collections.emptySet();
            }
        };
    }

    private String toCommaSeparatedString( List<String> list )
    {
        StringBuilder builder = new StringBuilder();
        for ( String string : list )
        {
            if ( builder.length() > 0 )
            {
                builder.append( "," );
            }
            builder.append( string );
        }
        return builder.toString();
    }

    protected Map<String, String> loadDatabasePropertiesFromFileInDatabaseDirectoryIfExists()
    {
        try
        {
            return load( propertiesFileLocation );
        }
        catch ( IOException e )
        {
            return stringMap();
        }
    }
}
