/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.remote;

import java.io.Serializable;

/**
 * Representation of the configuration supported by a client, for sending to the
 * server as part of the connection configuration phase.
 * 
 * NOTE: this is not specified yet.
 * 
 * @author Tobias Ivarsson
 */
public abstract class Configuration implements Serializable
{
    private static final long serialVersionUID = 1L;

    static Configuration of( ConfigurationModule module )
    {
        if ( module == null )
        {
            return EMPTY_CONFIGURATION;
        }
        else
        {
            return new ConfigurationImpl( module );
        }
    }

    private static final Configuration EMPTY_CONFIGURATION = new Configuration()
    {
        private static final long serialVersionUID = 1L;
    };

    private Configuration()
    {
    }

    private static final class ConfigurationImpl extends Configuration
    {
        private static final long serialVersionUID = 1L;

        ConfigurationImpl( ConfigurationModule module )
        {
            // TODO Auto-generated constructor stub
        }
    }
}
