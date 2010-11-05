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

package org.neo4j.server.webadmin;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.NeoServer;
import org.neo4j.server.database.DatabaseBlockedException;

/**
 * Used to get an {@link MBeanServer} instance for the currently connected
 * database. Also contains convinience methods for accessing MBeans for the
 * current database.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class MBeanServerFactory
{
    
    static Logger log = Logger.getLogger( MBeanServerFactory.class );

    /**
     * Used to check if the database has changed. If the database has changed,
     * the MBeanServer will be re-instantiated.
     */
    private static GraphDatabaseService cachedDb;

    /**
     * This will be changed if the database instance has changed, to allow
     * switching over to a remote database. This is used to allow hammering the
     * {@link #getServer()} method.
     */
    private static MBeanServerConnection cachedServer;

    public static MBeanServerConnection getServer()
    {

        GraphDatabaseService db; 
        try
        {
            db = NeoServer.server().database().db;
            //log.info(db);
            if ( db != cachedDb )
            {
                cachedDb = db;

                if ( db instanceof EmbeddedGraphDatabase )
                {
                    cachedServer = ManagementFactory.getPlatformMBeanServer();
                }
//                else if ( db instanceof RemoteGraphDatabase )
//                {
//                    try
//                    {
//                        JMXServiceURL address = new JMXServiceURL(
//                                ServerConfiguration.getInstance().get(
//                                        "general.jmx.uri" ).getValue() );
//
//                        JMXConnector connector = JMXConnectorFactory.connect(
//                                address, null );
//
//                        cachedServer = connector.getMBeanServerConnection();
//
//                    }
//                    catch ( MalformedURLException e )
//                    {
//                        // TODO Show proper error to user.
//                        throw new RuntimeException( e );
//                    }
//                    catch ( IOException e )
//                    {
//                        throw new RuntimeException(
//                                "Unable get JMX access to remote server, monitoring will be disabled.",
//                                e );
//                    }
//                }

            }
        }
        catch ( DatabaseBlockedException e1 )
        {
            e1.printStackTrace();
        }
        return cachedServer;

    }

}
