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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

import org.neo4j.desktop.ui.DesktopModel;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;

import static org.neo4j.helpers.collection.MapUtil.load;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

class DesktopConfigurator implements Configurator
{
    private final Map<String, String> map = new HashMap<String, String>();
    private final DesktopModel model;

    public DesktopConfigurator( DesktopModel model )
    {
        this.model = model;
        map.put( Configurator.DATABASE_LOCATION_PROPERTY_KEY, model.getDatabaseDirectory().getAbsolutePath() );
    }

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
    public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses()
    {
        return Collections.emptyList();
    }

    @Override
    public List<ThirdPartyJaxRsPackage> getThirdpartyJaxRsPackages()
    {
        return Collections.emptyList();
    }

    protected Map<String, String> loadDatabasePropertiesFromFileInDatabaseDirectoryIfExists()
    {
        try
        {
            return load( model.getDatabaseConfigurationFile() );
        }
        catch ( IOException e )
        {
            return stringMap();
        }
    }
}
