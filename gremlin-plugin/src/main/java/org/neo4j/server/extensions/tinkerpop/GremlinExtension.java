package org.neo4j.server.extensions.tinkerpop;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.graphdb.GraphDatabaseService;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;




@Path( "/gremlin" )
public class GremlinExtension
{
        private final GraphDatabaseService graphDb;
        private ScriptEngine engine;

        public GremlinExtension(@Context GraphDatabaseService graphDb, @Context UriInfo uriInfo )
        {
            this.graphDb = graphDb;
            ScriptEngineManager manager = new ScriptEngineManager();
            engine = manager.getEngineByName( "gremlin" );
            Neo4jGraph graph = new Neo4jGraph( graphDb );
            engine.getBindings( ScriptContext.ENGINE_SCOPE ).put( "g",
                    graph );
        }
     
        @GET
        @Produces( MediaType.TEXT_PLAIN )
        @Path( "/exec/{script}" )
        public Response addGreeting( @PathParam( "script" ) String script )
        {
            try
            {
                engine.eval( script );
            }
            catch ( ScriptException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return Response.ok( "executed " + script).build();
        }
}
