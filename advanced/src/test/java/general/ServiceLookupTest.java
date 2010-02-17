/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package general;

import java.io.File;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.remote.RemoteGraphDatabase;
import org.neo4j.remote.transports.LocalGraphDatabase;
import org.neo4j.remote.transports.RmiTransport;

public class ServiceLookupTest
{
	private static final String PATH = "target/neo";
	private static final String RMI_RESOURCE = "rmi://localhost/"
		+ ServiceLookupTest.class.getSimpleName();
	private static boolean rmi = true;
	
	@BeforeClass
	public static void setUp()
	{
		try
		{
			LocateRegistry.createRegistry( Registry.REGISTRY_PORT );
		}
		catch ( RemoteException e )
		{
			e.printStackTrace();
			rmi = false;
		}
	}

	@Test
	public void testLocalSite() throws Exception
	{
		GraphDatabaseService graphDb = new RemoteGraphDatabase(
				"file://" + new File(PATH).getAbsolutePath() );
		graphDb.shutdown();
	}

	@Test
	public void testRmiSite() throws Exception
	{
		Assume.assumeTrue( setupRmi() );
		GraphDatabaseService graphDb = new RemoteGraphDatabase( RMI_RESOURCE );
		graphDb.shutdown();
	}
	
	private static boolean setupRmi() throws Exception
	{
		try
		{
			RmiTransport.register( new LocalGraphDatabase( PATH ), RMI_RESOURCE );
		}
		catch ( ConnectException ex )
		{
			if ( rmi )
			{
				throw ex;
			}
			else
			{
				return false;
			}
		}
		return true;
	}

	@Ignore( "Not implemented" ) @Test
	public void testTcpSite() throws Exception
	{
		// TODO: set up server
		GraphDatabaseService graphDb = new RemoteGraphDatabase( "tcp://localhost" );
		graphDb.shutdown();
		// TODO: shut down server
	}
}
