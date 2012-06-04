package org.neo4j.server.web;

import static org.mockito.Mockito.mock;

import java.util.Arrays;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.Test;
import org.neo4j.kernel.GraphDatabaseAPI;
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
		GraphDatabaseAPI db = mock(GraphDatabaseAPI.class);
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
