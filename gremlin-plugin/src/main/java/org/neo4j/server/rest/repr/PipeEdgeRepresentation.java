package org.neo4j.server.rest.repr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
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
        GremlinPipeline<Vertex,Edge> curPipeline=(GremlinPipeline<Vertex,Edge>) pipeOfEdges.get(0);
        Iterator<Edge> curItr=curPipeline.iterator();
        while (curItr.hasNext())
        {
        	curEdge=(Edge) curItr.next();
        	Iterator<String> curKeySet=curEdge.getPropertyKeys().iterator();
        	while (curKeySet.hasNext())
        	{
        		curKey=curKeySet.next();
        		if (curKey.equalsIgnoreCase("name"))
        		{
        			curEdgeName=(String) curEdge.getProperty(curKey);
        			curEdgeMap.put(curEdgeName, curEdge);
        			break;
        		}
        	}
        	
        }

        return ValueRepresentation.string( "edges: " + curEdgeMap );
    }

}
