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
package org.neo4j.kernel.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Interface for specifying configuration parameters.
 *
 * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
 */
public interface ConfigParam
{
    /**
     * Add the configuration parameter(s) of this object to the specified map.
     *
     * @param config the map to add the parameter(s) of this object to.
     */
    void configure( Map<String, String> config );

    /**
     * Utility for converting {@link ConfigParam} objects to a Map of configuration parameter.s
     *
     * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
     */
    final class Conversion
    {
        /**
         * Create a new configuration map from a set of {@link ConfigParam} objects.
         *
         * @param params the parameters to add to the map.
         * @return a map containing the specified configuration parameters.
         */
        public static Map<String, String> create( ConfigParam... params )
        {
            return update( new HashMap<String, String>(), params );
        }

        /**
         * Updates a configuration map with the specified configuration parameters.
         *
         * @param config the map to update.
         * @param params the configuration parameters to update the map with.
         * @return the same configuration map as passed in.
         */
        public static Map<String, String> update( Map<String, String> config, ConfigParam... params )
        {
            if ( params != null ) for ( ConfigParam param : params )
                if ( param != null ) param.configure( config );
            return config;
        }
    }
}
