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

package org.neo4j.server.webadmin.task;

import java.io.IOException;

import org.neo4j.rest.WebServerFactory;
import org.neo4j.rest.domain.DatabaseLocator;
import org.neo4j.server.webadmin.AdminServer;
import org.neo4j.server.webadmin.utils.PlatformUtils;
import org.tanukisoftware.wrapper.WrapperManager;

/**
 * Triggers a full restart of the enclosing JVM upon instantiation.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class JvmRestartTask implements Runnable
{

    public synchronized void run()
    {
        try
        {

            if ( PlatformUtils.isProductionMode() )
            {
                // PRODUCTION MODE
                WrapperManager.restart();
            }
            else
            {
                // DEVELOPMENT MODE

                // Stop running servers
                System.out.println( "JVM Reboot. Shutting down server." );
                WebServerFactory.getDefaultWebServer().stopServer();
                AdminServer.INSTANCE.stopServer();
                DatabaseLocator.shutdownAndBlockGraphDatabase();

                if ( PlatformUtils.isWindows() )
                {
                    Runtime.getRuntime().exec( "start.bat" );
                }
                else
                {
                    Runtime.getRuntime().exec( "./start" );
                }

                // Sepukko!
                Runtime.getRuntime().exit( 0 );
            }
        }
        catch ( IOException e )
        {
            // TODO: Attempt to resolve the problems that are resolveable
            e.printStackTrace();
        }
    }
}
