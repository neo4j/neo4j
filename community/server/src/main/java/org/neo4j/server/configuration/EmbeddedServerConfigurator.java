/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.neo4j.kernel.AbstractGraphDatabase;

/**
 * Used by the EmbeddedNeoServer, which lets users
 * run the server from within their own application and 
 * using their own embedded database.
 */
public class EmbeddedServerConfigurator implements Configurator
{

    private MapBasedConfiguration config = new MapBasedConfiguration();
    
    public EmbeddedServerConfigurator(AbstractGraphDatabase db) {
        config.addProperty( DATABASE_LOCATION_PROPERTY_KEY, db.getStoreDir() );
    }
    
    @Override
    public Configuration configuration()
    {
        return config;
    }

    @Override
    public Map<String, String> getDatabaseTuningProperties()
    {
        return null;
    }

    @Override
    public Set<ThirdPartyJaxRsPackage> getThirdpartyJaxRsClasses()
    {
        return Collections.emptySet();
    }

}
