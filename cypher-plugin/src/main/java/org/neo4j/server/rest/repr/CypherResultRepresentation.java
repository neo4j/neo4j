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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;


public class CypherResultRepresentation extends ObjectRepresentation {

	private final ExecutionResult queryResult;

    public CypherResultRepresentation( ExecutionResult result )
    {
        super( RepresentationType.STRING );
        this.queryResult = result;
    }

    @Mapping("content")
    public Representation self() {
        
        Map<String, String> c = new HashMap<String, String>();
        c.put( "columns", queryResult.toString() );
        return MappingRepresentation.stringMap( STRING, c); 
    }
    
//    private void fillColumns() {
//        Collection<Node> nodeCollection = IteratorUtil.asCollection( asIterable( result.<Node>columnAs( returns ) ) );
//        if ( nodeCollection instanceof Iterable )
//        {
//            RepresentationType type = RepresentationType.STRING;
//            final List<Representation> results = new ArrayList<Representation>();
//            for ( final Object r : (Iterable) nodeCollection )
//            {
//                if ( r instanceof Node )
//                {
//                    type = RepresentationType.NODE;
//                    results.add( new NodeRepresentation( (Node) r ) );
//                }
//                else if ( r instanceof Relationship )
//                {
//                    type = RepresentationType.RELATIONSHIP;
//                    results.add( new RelationshipRepresentation(
//                            (Relationship) r ) );
//                }
//                else if ( r instanceof Double || r instanceof Float )
//                {
//                    type = RepresentationType.DOUBLE;
//                    results.add( ValueRepresentation.number( ( (Number) r ).doubleValue() ) );
//                }
//                else if ( r instanceof Long || r instanceof Integer )
//                {
//                    type = RepresentationType.LONG;
//                    results.add( ValueRepresentation.number( ( (Number) r ).longValue() ) );
//                }
//                else
//                {
//                    System.out.println( "Query: got back" + r );
//                    type = RepresentationType.STRING;
//                    results.add( ValueRepresentation.string( r.toString() ) );
//                }
//    }

    
    
}
