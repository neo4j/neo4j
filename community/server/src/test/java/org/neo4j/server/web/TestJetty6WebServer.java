/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.web;

import static org.mockito.Mockito.mock;

import java.util.Arrays;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.Test;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.WrappingNeoServer;

@Path("/")
public class TestJetty6WebServer {

	@GET
	public Response index()
	{
		return Response.status( Status.NO_CONTENT )
                .build();
	}
	
	@Test
	public void shouldBeAbleToRestart() throws Throwable
	{
		// TODO: This is needed because WebServer has a cyclic
		// dependency to NeoServer, which should be removed.
		// Once that is done, we should instantiate WebServer 
		// here directly.
        AbstractGraphDatabase db = mock(AbstractGraphDatabase.class);
		WrappingNeoServer neoServer = new WrappingNeoServer(db);
		
		WebServer server = neoServer.getWebServer();
		
		try 
		{
			server.setAddress("127.0.0.1");
			server.setPort(7878);
			
			server.addJAXRSPackages(Arrays.asList(new String[]{"org.neo4j.server.web"}), "/");
			
			server.start();
			server.stop();
			server.start();
		} finally 
		{
			try 
			{
				server.stop();
			} catch(Throwable t)
			{	
				
			}
		}
		
	}
	
}
