/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rest.repr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.neo4j.server.rest.repr.ObjectRepresentation;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jVertex;
import com.tinkerpop.gremlin.pipes.GremlinPipeline;
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
    	Neo4jVertex curVertex = null;
    	String curVertexName="";
    	String curKey="";
        GremlinPipeline<Vertex,Edge> curPipeline=(GremlinPipeline<Vertex, Edge>) pipeOfVertices.get(0);
        Iterator<Edge> curItr=curPipeline.iterator();
        while (curItr.hasNext())
        {
        	curVertex=(Neo4jVertex) curItr.next();
        	Iterator<String> curKeySet=curVertex.getPropertyKeys().iterator();
        	Object curValue=null;
        	LinkedHashMap<String,Object> individualVertexMap = new LinkedHashMap<String,Object>();
        	String nameOfVertex="";
        	while (curKeySet.hasNext())
        	{
        		curKey=curKeySet.next();
        		curValue=curVertex.getProperty(curKey);
        		if (curKey.equalsIgnoreCase("name"))
        			nameOfVertex=(String)curValue;
        		individualVertexMap.put(curKey, curValue);
        	}
        	curVertexMap.put(nameOfVertex, individualVertexMap);
        }

        return ValueRepresentation.string( "vertices: " + curVertexMap );
    }
}
