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

package examples;

// START SNIPPET: class
import java.net.MalformedURLException;
import java.rmi.RemoteException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.remote.transports.LocalGraphDatabase;
import org.neo4j.remote.transports.RmiTransport;

public class ThatStartsARemoteGraphDatabaseRMIServer
{
    private static final String RESOURCE_URI = "rmi://rmi-server/neo4j-graphdb";

    public static void publishServer( GraphDatabaseService neo )
            throws MalformedURLException, RemoteException
    {
        RmiTransport.register( new LocalGraphDatabase( neo ), RESOURCE_URI );
    }
}
// END SNIPPET: class
