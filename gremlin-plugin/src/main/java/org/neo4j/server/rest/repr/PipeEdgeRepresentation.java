package org.neo4j.server.rest.repr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.neo4j.server.rest.repr.ObjectRepresentation.Mapping;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jVertex;
import com.tinkerpop.gremlin.pipes.GremlinPipeline;

public class PipeEdgeRepresentation extends ObjectRepresentation {

	private final ArrayList<Edge> pipeOfEdges;

    public PipeEdgeRepresentation( ArrayList<Edge> pipeOfEdges )
    {
        super( RepresentationType.RELATIONSHIP );
        this.pipeOfEdges = pipeOfEdges;
    }

    @Mapping("self")
    public ValueRepresentation selfUri() {
        LinkedHashMap<String,Object> curEdgeMap = new LinkedHashMap<String,Object>();
    	Edge curEdge = null;
    	String curEdgeName="";
    	String curKey="";
        GremlinPipeline<Vertex,Edge> curPipeline=(GremlinPipeline<Vertex, Edge>) pipeOfEdges.get(0);
        Iterator<Edge> curItr=curPipeline.iterator();
        while (curItr.hasNext())
        {
        	curEdge=(Edge) curItr.next();
        	Iterator<String> curKeySet=curEdge.getPropertyKeys().iterator();
        	Object curValue=null;
        	LinkedHashMap<String,Object> individualEdgeMap = new LinkedHashMap<String,Object>();
        	String nameOfVertex="";
        	while (curKeySet.hasNext())
        	{
        		curKey=curKeySet.next();
        		curValue=curEdge.getProperty(curKey);
        		if (curKey.equalsIgnoreCase("name"))
        			nameOfVertex=(String)curValue;
        		individualEdgeMap.put(curKey, curValue);
        	}
        	curEdgeMap.put(nameOfVertex, individualEdgeMap);
        }

        return ValueRepresentation.string( "edges: " + curEdgeMap );
    }

}
