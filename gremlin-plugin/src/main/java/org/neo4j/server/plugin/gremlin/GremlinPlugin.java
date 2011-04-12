package org.neo4j.server.plugin.gremlin;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jEdge;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jVertex;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.server.rest.repr.DatabaseRepresentation;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ValueRepresentation;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;

/* This is a class that will represent a server side
 * gremlin plugin and will run through and return json
 * for the following use cases:
 * Add/delete vertices and edges from the graph.
 * Manipulate the graph indices.
 * Search for elements of a graph.
 * Load graph data from a file or URL.
 * Make use of JUNG algorithms.
 * Make use of SPARQL queries over OpenRDF-based graphs.
 * and much, much more.
 */

@Description("A server side gremlin plugin for the neo4j REST server that will perform various node traversals and searches")
public class GremlinPlugin extends ServerPlugin {
    /**
     * This will give a plugin of type
     * curl -d 'script=g.v(0).outE' http://localhost:7474/db/data/ext/GremlinPlugin/node/0/execute_from_node
     *
     * @param script
     * @return
     */
    @Name("execute_script")
    @Description("execute a Gremlin script with 'g' set to the Neo4jGraph and 'results' containing a resulting vertex")
    @PluginTarget(GraphDatabaseService.class)
    public Representation executeScript(@Source final GraphDatabaseService neo4j, @Description("The Gremlin script") @Parameter(name = "script", optional = true) final String script) {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("gremlin");
        Neo4jGraph graph = new Neo4jGraph(neo4j);
        engine.getBindings(ScriptContext.ENGINE_SCOPE).put("g", graph);

        try {
            Object result = engine.eval(script);
            if (result instanceof Iterable) {
                List<Representation> results = new ArrayList<Representation>();
                for (Object r : (Iterable) result) {
                    if (r instanceof Vertex) {
                        results.add(new NodeRepresentation(((Neo4jVertex) r).getRawVertex()));
                    } else if (r instanceof Edge) {
                        results.add(new RelationshipRepresentation(((Neo4jEdge) r).getRawEdge()));
                    } else if (r instanceof Graph) {
                        results.add(new DatabaseRepresentation(graph.getRawGraph()));
                    } else {
                        results.add(ValueRepresentation.string(r.toString()));
                    }
                }
                return new ListRepresentation(RepresentationType.NODE, results);
            } else {
                return ValueRepresentation.string(result.toString());
            }
        } catch (ScriptException e) {
            return ValueRepresentation.string(e.getMessage());
        }
    }


}
