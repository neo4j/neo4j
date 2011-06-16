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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import com.tinkerpop.gremlin.pipes.util.Table;

public class GremlinTableRepresentation extends ObjectRepresentation
{

    private final Table queryResult;

    public GremlinTableRepresentation( Table result )
    {
        super( RepresentationType.STRING );
        this.queryResult = result;
    }

    @Mapping( "columns" )
    public Representation columns()
    {

        return ListRepresentation.string( queryResult.getColumnNames() );
    }

//    @Mapping( "data" )
//    public Representation data()
//    {
//        // rows
//        List<Representation> rows = new ArrayList<Representation>();
//        for ( Map<String, Object> row : queryResult. )
//        {
//            List<Representation> fields = new ArrayList<Representation>();
//            // columns
//            for ( String column : queryResult.columns() )
//            {
//                Representation rowRep = getRepresentation( row.get( column ) );
//                fields.add( rowRep );
//
//            }
//            rows.add( new ListRepresentation( "row", fields ) );
//        }
//        return new ListRepresentation( "data", rows );
//    }

    // private void fillColumns() {
    // Collection<Node> nodeCollection = IteratorUtil.asCollection( asIterable(

    private Representation getRepresentation( Object r )
    {
        if(r == null ) {
            return ValueRepresentation.string( null );
        }
        if ( r instanceof Node )
        {
            return new NodeRepresentation( (Node) r );
        }
        if ( r instanceof Relationship )
        {
            return new RelationshipRepresentation( (Relationship) r );
        }
        else if ( r instanceof Double || r instanceof Float )
        {
            return ValueRepresentation.number( ( (Number) r ).doubleValue() );
        }
        else if ( r instanceof Long || r instanceof Integer )
        {
            return ValueRepresentation.number( ( (Number) r ).longValue() );
        }
        else
        {
            return ValueRepresentation.string( r.toString() );
        }
    }

}
