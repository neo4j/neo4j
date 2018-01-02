/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.NeoServer;
import org.neo4j.server.advanced.AdvancedBootstrapper;

import static org.neo4j.server.web.ServerInternalSettings.SERVER_CONFIG_FILE;
import static org.neo4j.server.web.ServerInternalSettings.SERVER_CONFIG_FILE_KEY;

public class EnterpriseBootstrapper extends AdvancedBootstrapper
{
    public static void main( String[] args )
    {
        int exit = start( new EnterpriseBootstrapper(), args );
        if ( exit != 0 )
        {
            System.exit( exit );
        }
    }

    @Override
    protected NeoServer createNeoServer( Config configurator, GraphDatabaseDependencies dependencies, LogProvider
            userLogProvider )
    {
        return new EnterpriseNeoServer( configurator, dependencies, userLogProvider );
    }

    @Override
    protected Config createConfig( Log log, File file, Pair<String,String>[] configOverrides ) throws IOException
    {
        return new EnterpriseServerConfigLoader().loadConfig( file, new File( System.getProperty( SERVER_CONFIG_FILE_KEY, SERVER_CONFIG_FILE ) ), log, configOverrides );
    }
}
