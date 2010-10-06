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

package org.neo4j.remote.transports;

import static java.rmi.registry.LocateRegistry.createRegistry;
import static java.rmi.registry.LocateRegistry.getRegistry;

import java.rmi.RemoteException;
import java.rmi.registry.Registry;

import org.junit.BeforeClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.remote.RemoteGraphDbTransportTestSuite;

public class RmiTransportTest extends RemoteGraphDbTransportTestSuite
{
    private static final String RESOURCE_URI = "rmi://localhost/Neo4jGraphDatabase";

    @BeforeClass
    public static void setupRmiRegistry() throws Exception
    {
        try
        {
            createRegistry( Registry.REGISTRY_PORT );
        }
        catch ( RemoteException e )
        {
            getRegistry( Registry.REGISTRY_PORT );
        }
    }

    @Override
    protected String createServer( GraphDatabaseService graphDb,
            IndexService index )
            throws Exception
    {
        RmiTransport.register( basicServer( graphDb, index ), RESOURCE_URI );
        return RESOURCE_URI;
    }
}
