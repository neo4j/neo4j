/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.AbstractConfiguration;

public class MapBasedConfiguration extends AbstractConfiguration
{

    private Map<String, Object> config = new HashMap<String, Object>();

    @Override
    public boolean isEmpty()
    {
        return config.isEmpty();
    }

    @Override
    public boolean containsKey( String key )
    {
        return config.containsKey( key );
    }

    @Override
    public Object getProperty( String key )
    {
        return config.get( key );
    }

    @Override
    public Iterator<String> getKeys()
    {
        return config.keySet()
                .iterator();
    }

    @Override
    protected void addPropertyDirect( String key, Object value )
    {
        config.put( key, value );
    }

}
