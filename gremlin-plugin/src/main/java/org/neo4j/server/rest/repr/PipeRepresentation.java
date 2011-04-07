package org.neo4j.server.rest.repr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.neo4j.server.rest.repr.ObjectRepresentation;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.pipes.Pipe;

public class PipeRepresentation extends ObjectRepresentation {
	
	private final ArrayList<Vertex> pipeOfVertices;

    public PipeRepresentation( ArrayList<Vertex> pipeOfVertices )
    {
        super( RepresentationType.NODE );
        this.pipeOfVertices = pipeOfVertices;
    }

    @Mapping("self")
    public ValueRepresentation selfUri() {
        LinkedHashMap<String,Object> curVertexMap = new LinkedHashMap<String,Object>();
    	Vertex curVertex = null;
    	String curVertexName="";
        for (int i=0;i<pipeOfVertices.size();i++)
        {
        	curVertex=pipeOfVertices.get(i);
        	curVertexName=(String)curVertex.getProperty("name");
        	curVertexMap.put(curVertexName, curVertex);
        }

        return ValueRepresentation.string( "edge: " + curVertexMap );
    }
}
