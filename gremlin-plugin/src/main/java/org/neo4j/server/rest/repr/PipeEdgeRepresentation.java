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
