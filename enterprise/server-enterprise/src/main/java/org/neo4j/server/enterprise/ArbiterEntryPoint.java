/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.enterprise;

import java.io.File;
import java.io.IOException;

import org.neo4j.server.configuration.ServerSettings;

public class ArbiterEntryPoint
{
    public static void main( String[] args ) throws IOException
    {
        int status = new ArbiterBootstrapper().start( getConfigFile() );
        if ( status != 0 )
        {
            System.exit( status );
        }
    }

    static File getConfigFile()
    {
        String configPath = System.getProperty( ServerSettings.SERVER_CONFIG_FILE_KEY );
        if ( configPath == null )
        {
            throw new RuntimeException( "System property " + ServerSettings.SERVER_CONFIG_FILE_KEY +
                    " must be provided" );
        }

        File configFile = new File( configPath );
        if ( !configFile.exists() )
        {
            throw new IllegalArgumentException( configFile + " doesn't exist" );
        }
        return configFile;
    }
}
