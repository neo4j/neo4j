package org.neo4j.server.web;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.Test;

import scala.actors.threadpool.Arrays;

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
		WebServer server = new Jetty6WebServer();
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
			server.stop();
		}
		
	}
	
}
