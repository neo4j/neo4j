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
package org.neo4j.helpers;

import org.junit.rules.ExternalResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;

public class Configuration extends ExternalResource
{
    public static final String DEFAULT = null;
    private final Map<String, String> configuration = new HashMap<>();
    private final Map<String, String> sysProperties = new HashMap<>();

    public Config config( LoadableConfig settingsClasses )
    {
        return Config.builder().withSettings( configuration ).withConfigClasses(
                Collections.singletonList( settingsClasses ) ).build();
    }

    public Configuration with( Setting<?> setting, String value )
    {
        String key = setting.name();
        if ( value == null )
        {
            configuration.remove( key );
        }
        else
        {
            configuration.put( key, value );
        }
        return this;
    }

    public Configuration withSystemProperty( String key, String value )
    {
        value = sysProperties.put( key, updateSystemProperty( key, value ) );
        if ( value != null )
        {
            // restore before we throw
            sysProperties.remove( key );
            updateSystemProperty( key, value );
            throw new IllegalArgumentException( "Cannot update '" + key + "' more than once." );
        }
        return this;
    }

    @Override
    protected void after()
    {
        for ( Map.Entry<String, String> entry : sysProperties.entrySet() )
        {
            updateSystemProperty( entry.getKey(), entry.getValue() );
        }
    }

    private static String updateSystemProperty( String key, String value )
    {
        if ( value == null )
        {
            return System.clearProperty( key );
        }
        else
        {
            return System.setProperty( key, value );
        }
    }
}
